package com.shan.kafka.consumerservice.kafka;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

/**
 * data-model.md {@code ConsumerRuntimeState}: running counters driven by {@link ProcessingOutcome}
 * (FR-021). Served as-is by the future status endpoint (T049); never performs a live Kafka check
 * itself.
 */
@Component
public class ConsumerRuntimeState {

    private final AtomicLong messagesConsumed = new AtomicLong();
    private final AtomicLong messagesRejected = new AtomicLong();
    private volatile String lastError;

    public void record(ProcessingOutcome outcome) {
        switch (outcome.outcome()) {
            case PROCESSED -> messagesConsumed.incrementAndGet();
            case INVALID_DESERIALIZATION, INVALID_VALIDATION -> {
                messagesRejected.incrementAndGet();
                lastError = outcome.detail();
            }
        }
    }

    /**
     * For conditions that aren't a per-message outcome at all (e.g. a broker connectivity
     * failure, FR-024) — surfaces via {@code lastError} without touching the message counters,
     * unlike {@link #record(ProcessingOutcome)}.
     */
    public void recordError(String detail) {
        lastError = detail;
    }

    public long getMessagesConsumed() {
        return messagesConsumed.get();
    }

    public long getMessagesRejected() {
        return messagesRejected.get();
    }

    public String getLastError() {
        return lastError;
    }
}
