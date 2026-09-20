package com.shan.kafka.producerservice.kafka;

/**
 * Wire contract with consumer-service, per contracts/kafka-message-contract.md. producer-service
 * and consumer-service each keep their own copy of this shape (constitution Principle IV); the
 * JSON they exchange is the only thing that has to match.
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
