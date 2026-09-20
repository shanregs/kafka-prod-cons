package com.shan.kafka.consumerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.shan.kafka.consumerservice.support.ConsumerServiceIntegrationTest;

/**
 * T035 (US3): FR-014, SC-008, kafka-message-contract.md Delivery semantics — a record whose value
 * is not valid JSON at all fails deserialization before any KafkaMessageEnvelope can exist (the
 * listener method body is never invoked for it); it must be logged/handled and acknowledged
 * (skipped), and the next, valid message must still be consumed normally.
 */
class MalformedMessageTest extends ConsumerServiceIntegrationTest {

    @Test
    void malformedRecordIsRejectedAndDoesNotBlockTheNextValidMessage() throws Exception {
        long rejectedBefore = runtimeState.getMessagesRejected();
        long consumedBefore = runtimeState.getMessagesConsumed();

        publishRaw("this is not json at all {{{");
        publishValidEnvelope(1, "after-malformed");

        awaitCondition(() -> runtimeState.getMessagesRejected() > rejectedBefore
                && runtimeState.getMessagesConsumed() > consumedBefore, 10_000);

        assertThat(runtimeState.getMessagesRejected()).isGreaterThan(rejectedBefore);
        assertThat(runtimeState.getMessagesConsumed()).isGreaterThan(consumedBefore);
        assertThat(runtimeState.getLastError()).isNotBlank();
    }
}
