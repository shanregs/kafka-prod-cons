package com.shan.kafka.consumerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import com.shan.kafka.consumerservice.support.ConsumerServiceIntegrationTest;

/**
 * T038 (US3): kafka-message-contract.md Delivery semantics — on restart, already-acknowledged
 * messages are not redelivered; only messages left unacknowledged before the restart may be
 * redelivered. Restarting the listener CONTAINER (rather than the whole Spring context) is
 * sufficient and faithful: committed offsets live on the broker, not in the container, so this
 * exercises exactly the property under test without needing a full process restart.
 */
class RedeliveryOnRestartTest extends ConsumerServiceIntegrationTest {

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Test
    void restartingTheContainerDoesNotRedeliverAlreadyAcknowledgedMessages() throws Exception {
        long consumedBefore = runtimeState.getMessagesConsumed();

        publishValidEnvelope(1, "before-restart");
        awaitCondition(() -> runtimeState.getMessagesConsumed() > consumedBefore, 10_000);
        long consumedAfterFirst = runtimeState.getMessagesConsumed();
        assertThat(consumedAfterFirst).isGreaterThan(consumedBefore);

        MessageListenerContainer container = registry.getListenerContainers().iterator().next();
        container.stop();
        container.start();

        // Give the restarted container a moment to resume polling, then confirm the
        // already-acknowledged message was NOT redelivered (count unchanged)...
        Thread.sleep(2000);
        assertThat(runtimeState.getMessagesConsumed()).isEqualTo(consumedAfterFirst);

        // ...and that a NEW message published after the restart is still picked up normally.
        publishValidEnvelope(2, "after-restart");
        awaitCondition(() -> runtimeState.getMessagesConsumed() > consumedAfterFirst, 10_000);
        assertThat(runtimeState.getMessagesConsumed()).isGreaterThan(consumedAfterFirst);
    }
}
