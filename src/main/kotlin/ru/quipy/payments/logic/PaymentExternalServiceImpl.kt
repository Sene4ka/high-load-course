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
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore


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

        private const val MAX_ATTEMPTS = 3
        private const val RETRY_DELAY_MS = 100L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val inFlight = Semaphore(properties.parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1),
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        inFlight.acquire()
        rateLimiter.tickBlocking()

        try {
            var attempt = 0
            var finalSuccess = false
            var finalReason: String? = null

            while (attempt < MAX_ATTEMPTS && !finalSuccess) {
                attempt++
                try {
                    val request = Request.Builder().run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                    }.build()

                    val body = client.newCall(request).execute().use { response ->
                        try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Parse failed, attempt $attempt, txId: $transactionId", e)
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, attempt: $attempt, succeeded: ${body.result}, message: ${body.message}")

                    if (body.result) {
                        finalSuccess = true
                        finalReason = body.message
                    } else if (body.message?.contains("Temporary", ignoreCase = true) == true) {
                        logger.warn("[$accountName] Temporary error, retrying (attempt $attempt/$MAX_ATTEMPTS)")
                        finalReason = body.message
                        if (attempt < MAX_ATTEMPTS) Thread.sleep(RETRY_DELAY_MS)
                    } else {
                        finalReason = body.message
                        break
                    }
                } catch (e: SocketTimeoutException) {
                    logger.warn("[$accountName] Timeout, attempt $attempt/$MAX_ATTEMPTS", e)
                    finalReason = "Request timeout."
                    if (attempt < MAX_ATTEMPTS) Thread.sleep(RETRY_DELAY_MS)
                } catch (e: Exception) {
                    logger.error("[$accountName] Unexpected error, attempt $attempt, txId: $transactionId", e)
                    finalReason = e.message
                    break
                }
            }

            if (finalSuccess) {
                paymentMetrics.processedSuccess()
            } else {
                paymentMetrics.processedFailure(finalReason ?: "max retries exceeded")
            }

            paymentESService.update(paymentId) {
                it.logProcessing(finalSuccess, now(), transactionId, reason = finalReason)
            }
        } finally {
            inFlight.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()