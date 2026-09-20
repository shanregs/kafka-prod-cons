package com.shan.kafka.consumerservice.kafka;

import org.springframework.stereotype.Component;

/**
 * contracts/kafka-message-contract.md's 3-point validity rule: envelope fields present AND
 * payload.content present and non-empty.
 */
@Component
public class MessageValidator {

    /** @return {@code null} if valid, otherwise a human-readable reason it's invalid. */
    public String validate(KafkaMessageEnvelope envelope) {
        if (envelope == null) {
            return "envelope is null";
        }
        if (isBlank(envelope.messageId())) {
            return "missing messageId";
        }
        if (isBlank(envelope.producedAt())) {
            return "missing producedAt";
        }
        if (isBlank(envelope.producerId())) {
            return "missing producerId";
        }
        if (envelope.sequenceNumber() < 1) {
            return "missing or invalid sequenceNumber";
        }
        if (envelope.payload() == null || isBlank(envelope.payload().content())) {
            return "missing or empty payload.content";
        }
        return null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
