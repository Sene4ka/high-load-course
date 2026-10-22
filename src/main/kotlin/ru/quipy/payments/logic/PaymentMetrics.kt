package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

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
}