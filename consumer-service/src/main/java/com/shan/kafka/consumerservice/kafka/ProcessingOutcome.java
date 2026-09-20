package com.shan.kafka.consumerservice.kafka;

import java.time.Instant;

/**
 * data-model.md {@code ProcessingOutcome}: the result of handling one consumed record.
 * {@code INVALID_DESERIALIZATION} originates from the container-level error handler (no envelope
 * object exists); {@code PROCESSED}/{@code INVALID_VALIDATION} originate from the listener's
 * normal path after a successful deserialization.
 */
public record ProcessingOutcome(String messageId, Outcome outcome, String detail, Instant processedAt) {

    public enum Outcome { PROCESSED, INVALID_DESERIALIZATION, INVALID_VALIDATION }

    public static ProcessingOutcome processed(String messageId) {
        return new ProcessingOutcome(messageId, Outcome.PROCESSED, null, Instant.now());
    }

    public static ProcessingOutcome invalidValidation(String messageId, String detail) {
        return new ProcessingOutcome(messageId, Outcome.INVALID_VALIDATION, detail, Instant.now());
    }

    public static ProcessingOutcome invalidDeserialization(String detail) {
        return new ProcessingOutcome(null, Outcome.INVALID_DESERIALIZATION, detail, Instant.now());
    }
}
