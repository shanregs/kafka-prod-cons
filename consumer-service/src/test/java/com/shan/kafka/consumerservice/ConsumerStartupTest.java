package com.shan.kafka.consumerservice;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;

import com.shan.kafka.consumerservice.support.ConsumerServiceIntegrationTest;

/**
 * T037 (US3): FR-012 — consumer-service starts cleanly with no messages on the topic and no
 * producer-service running or reachable. The mere fact that this Spring context starts
 * successfully (no producer-service anywhere in the picture) demonstrates FR-012; the assertions
 * just confirm the resulting counters are the expected zero/empty baseline. {@code @DirtiesContext}
 * forces a genuinely fresh context/counters here — the base's context (and its
 * {@code ConsumerRuntimeState} singleton) is otherwise reused across every test extending it.
 */
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
class ConsumerStartupTest extends ConsumerServiceIntegrationTest {

    @Test
    void startsCleanlyWithNothingOnTheTopicAndNoProducer() {
        assertThat(runtimeState.getMessagesConsumed()).isZero();
        assertThat(runtimeState.getMessagesRejected()).isZero();
        assertThat(runtimeState.getLastError()).isNull();
    }
}
