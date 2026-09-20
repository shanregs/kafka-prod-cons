package com.shan.kafka.consumerservice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * T039/T041/T042: consumes from the configured topic, validates (T040), logs, records the
 * {@link ProcessingOutcome}, and acknowledges — every outcome (valid or invalid) is acknowledged,
 * so an invalid message can never block subsequent valid ones (FR-014, SC-008, data-model.md
 * Acknowledgement rule). Deserialization failures never reach this method at all (see
 * {@link ConsumerKafkaConfig}'s error handler) — this method only ever sees a fully-deserialized
 * {@link KafkaMessageEnvelope}.
 */
@Component
public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private final MessageValidator validator;
    private final ConsumerRuntimeState runtimeState;

    public MessageProcessor(MessageValidator validator, ConsumerRuntimeState runtimeState) {
        this.validator = validator;
        this.runtimeState = runtimeState;
    }

    @KafkaListener(topics = "${app.kafka.topic}")
    public void onMessage(KafkaMessageEnvelope envelope, Acknowledgment acknowledgment) {
        String validationError = validator.validate(envelope);

        if (validationError == null) {
            log.info("Processed message {} (sequence {})", envelope.messageId(), envelope.sequenceNumber());
            runtimeState.record(ProcessingOutcome.processed(envelope.messageId()));
        } else {
            String messageId = envelope != null ? envelope.messageId() : null;
            log.warn("Rejected invalid message {}: {}", messageId, validationError);
            runtimeState.record(ProcessingOutcome.invalidValidation(messageId, validationError));
        }

        acknowledgment.acknowledge();
    }
}
