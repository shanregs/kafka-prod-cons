package com.shan.kafka.consumerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.shan.kafka.consumerservice.support.ConsumerServiceIntegrationTest;

/**
 * T034 (US3): FR-013, FR-015 — a valid message is consumed, logged, and increments
 * messagesConsumed.
 */
class ValidMessageConsumptionTest extends ConsumerServiceIntegrationTest {

    @Test
    void validMessageIsConsumedAndCounted() throws Exception {
        long before = runtimeState.getMessagesConsumed();

        publishValidEnvelope(1, "hello");

        awaitCondition(() -> runtimeState.getMessagesConsumed() > before, 10_000);

        assertThat(runtimeState.getMessagesConsumed()).isGreaterThan(before);
    }
}
