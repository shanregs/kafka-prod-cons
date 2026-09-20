package com.shan.kafka.consumerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.shan.kafka.consumerservice.support.ConsumerServiceIntegrationTest;

/**
 * T036 (US3): FR-014, SC-008 — a message that deserializes successfully but fails validation (a
 * missing/empty payload.content here) is logged/handled and acknowledged, and does not stop
 * consumption of the next valid message.
 */
class InvalidPayloadMessageTest extends ConsumerServiceIntegrationTest {

    @Test
    void invalidPayloadIsRejectedAndDoesNotBlockTheNextValidMessage() throws Exception {
        long rejectedBefore = runtimeState.getMessagesRejected();
        long consumedBefore = runtimeState.getMessagesConsumed();

        // Deserializes fine (valid JSON, matches the envelope shape) but payload.content is empty.
        publishValidEnvelope(1, "");
        publishValidEnvelope(2, "valid-after-invalid-payload");

        awaitCondition(() -> runtimeState.getMessagesRejected() > rejectedBefore
                && runtimeState.getMessagesConsumed() > consumedBefore, 10_000);

        assertThat(runtimeState.getMessagesRejected()).isGreaterThan(rejectedBefore);
        assertThat(runtimeState.getMessagesConsumed()).isGreaterThan(consumedBefore);
    }
}
