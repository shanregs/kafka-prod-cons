package com.shan.kafka.consumerservice.kafka;

/**
 * Wire contract with producer-service, per contracts/kafka-message-contract.md. Mirrors
 * producer-service's type field-for-field (constitution Principle IV: no shared module) — only
 * the JSON shape has to match. Fields are nullable/defaultable here on purpose: an incoming
 * record with a missing field deserializes with that field null (or 0 for sequenceNumber, which
 * validation also rejects since valid sequence numbers are ≥1), so a later validation step can
 * distinguish and report "missing" rather than the deserializer throwing.
 */
public record KafkaMessageEnvelope(
        String messageId,
        String producedAt,
        String producerId,
        long sequenceNumber,
        MessagePayload payload) {

    public record MessagePayload(String content) {
    }
}
