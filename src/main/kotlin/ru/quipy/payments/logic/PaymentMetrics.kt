package ru.quipy.payments.logic

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PostConstruct
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit

@Component
class PaymentMetrics(
    private val registry: MeterRegistry,
) {
    fun submitted() {
        registry.counter(
            "payment_flow_total",
            "stage", "submit",
            "outcome", "received",
        ).increment()
    }

    fun rejected() {
        registry.counter(
            "payment_flow_total",
            "stage", "submit",
            "outcome", "rejected",
        ).increment()
    }

    fun processedSuccess() {
        registry.counter(
            "payment_flow_total",
            "stage", "process",
            "outcome", "success",
        ).increment()
    }

    fun processedFailure(reason: String) {
        registry.counter(
            "payment_flow_total",
            "stage", "process",
            "outcome", reason,
        ).increment()
    }

    fun retried(attempt: Int, reason: String) {
        registry.counter(
            "payment_retry_total",
            "attempt", attempt.toString(),
            "reason", reason,
        ).increment()
    }

    @PostConstruct
    fun initMetrics() {
        registry.counter("payment_retry_total", "attempt", "0", "reason", "init")
    }

    fun externalCall(durationMs: Long, outcome: String) {
        Timer.builder("payment_external_call_duration")
            .tag("outcome", outcome)
            .publishPercentiles(0.5, 0.9, 0.95, 0.99)
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofMillis(50))
            .maximumExpectedValue(Duration.ofSeconds(10))
            .register(registry)
            .record(durationMs, TimeUnit.MILLISECONDS)
    }

    fun attemptsPerPayment(attempts: Int, success: Boolean) {
        DistributionSummary.builder("payment_attempts_per_payment")
            .tag("result", if (success) "success" else "fail")
            .register(registry)
            .record(attempts.toDouble())
    }
}