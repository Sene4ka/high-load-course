package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentMetrics: PaymentMetrics,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val ATTEMPT_TIMEOUT_MS = 1500L
        private const val MIN_ATTEMPT_MS = 1100L
        private const val SAFETY_MARGIN_MS = 100L
        private const val NETWORK_MARGIN_MS = 400L
        private const val MAX_ATTEMPTS = 5
        private const val BASE_BACKOFF_MS = 50L
        private const val MAX_BACKOFF_MS = 500L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .connectTimeout(500, TimeUnit.MILLISECONDS)
        .readTimeout(10, TimeUnit.SECONDS)
        .build()

    private val inFlight = Semaphore(properties.parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1),
    )

    private sealed interface AttemptResult {
        data class Success(val message: String?) : AttemptResult
        data class Retryable(val reason: String, val retryAfterMs: Long? = null) : AttemptResult
        data class Fatal(val reason: String) : AttemptResult
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var attempt = 0
        var success = false
        var reason: String? = null

        inFlight.acquire()
        try {
            while (attempt < MAX_ATTEMPTS) {
                if (!waitRateLimit(deadline)) {
                    reason = reason ?: "deadline exceeded before attempt"
                    break
                }

                val remaining = deadline - now() - SAFETY_MARGIN_MS
                val attemptTimeout = minOf(ATTEMPT_TIMEOUT_MS, remaining)
                attempt++

                when (val r = callOnce(paymentId, transactionId, amount, attemptTimeout)) {
                    is AttemptResult.Success -> { success = true; reason = r.message; break }
                    is AttemptResult.Fatal -> { reason = r.reason; break }
                    is AttemptResult.Retryable -> {
                        reason = r.reason
                        if (attempt >= MAX_ATTEMPTS) break
                        val delay = r.retryAfterMs ?: backoff(r.reason, attempt)
                        if (deadline - now() - SAFETY_MARGIN_MS - delay < MIN_ATTEMPT_MS) {
                            logger.warn("[$accountName] No time left for retry, payment $paymentId")
                            break
                        }
                        paymentMetrics.retried(attempt + 1, r.reason)
                        if (delay > 0) Thread.sleep(delay)
                    }
                }
            }
        } finally {
            inFlight.release()
        }

        if (success) paymentMetrics.processedSuccess() else paymentMetrics.processedFailure(reason ?: "unknown")
        paymentMetrics.attemptsPerPayment(attempt, success)

        paymentESService.update(paymentId) {
            it.logProcessing(success, now(), transactionId, reason = reason)
        }
    }

    private fun waitRateLimit(deadline: Long): Boolean {
        while (!rateLimiter.tick()) {
            if (deadline - now() - SAFETY_MARGIN_MS < MIN_ATTEMPT_MS) return false
            Thread.sleep(5)
        }
        return deadline - now() - SAFETY_MARGIN_MS >= MIN_ATTEMPT_MS
    }

    private fun callOnce(paymentId: UUID, transactionId: UUID, amount: Int, timeoutMs: Long): AttemptResult {
        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${Duration.ofMillis(timeoutMs - NETWORK_MARGIN_MS)}")
            post(emptyBody)
        }.build()

        val call = client.newCall(request)
        call.timeout().timeout(timeoutMs, TimeUnit.MILLISECONDS)

        val startedAt = now()
        return try {
            call.execute().use { response ->
                val code = response.code
                val result: AttemptResult = when {
                    code == 429 -> AttemptResult.Retryable("http_429", parseRetryAfter(response.header("Retry-After")))
                    code == 500 || code == 502 || code == 503 || code == 504 -> AttemptResult.Retryable("http_$code")
                    code == 408 -> AttemptResult.Retryable("server_timeout")
                    code in 400..499 -> AttemptResult.Fatal("http_$code")
                    else -> {
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Parse failed, txId: $transactionId", e)
                            null
                        }
                        when {
                            body == null -> AttemptResult.Fatal("parse_error")
                            body.result -> AttemptResult.Success(body.message)
                            isTemporary(body.message) -> AttemptResult.Retryable("temporary")
                            else -> AttemptResult.Fatal(body.message ?: "unknown")
                        }
                    }
                }
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, code: $code, result: $result")
                paymentMetrics.externalCall(now() - startedAt, outcomeTag(result))
                result
            }
        } catch (e: InterruptedIOException) {
            logger.warn("[$accountName] Timeout ${timeoutMs}ms, txId: $transactionId")
            paymentMetrics.externalCall(now() - startedAt, "client_timeout")
            AttemptResult.Retryable("client_timeout")
        } catch (e: IOException) {
            logger.error("[$accountName] IO error, txId: $transactionId", e)
            paymentMetrics.externalCall(now() - startedAt, "io_error")
            AttemptResult.Retryable("io_error")
        }
    }

    private fun isTemporary(msg: String?) = msg != null && (
            msg.contains("temporary", true) || msg.contains("timeout", true) || msg.contains("timed out", true))

    private fun backoff(reason: String, attempt: Int): Long {
        if (reason == "client_timeout" || reason == "server_timeout") return 0
        val exp = minOf(MAX_BACKOFF_MS, BASE_BACKOFF_MS * (1L shl (attempt - 1)))
        return exp / 2 + ThreadLocalRandom.current().nextLong(exp / 2 + 1)
    }

    private fun parseRetryAfter(header: String?): Long? {
        val v = header?.trim()?.toLongOrNull() ?: return null
        return if (v > 1_000_000_000_000L) maxOf(0, v - now()) else v * 1000
    }

    private fun outcomeTag(r: AttemptResult) = when (r) {
        is AttemptResult.Success -> "success"
        is AttemptResult.Retryable -> r.reason
        is AttemptResult.Fatal -> "fatal"
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()