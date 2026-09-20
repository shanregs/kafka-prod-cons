package com.shan.kafka.consumerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import com.shan.kafka.consumerservice.support.ConsumerServiceIntegrationTest;

/**
 * T055 (Polish): FR-025, SC-011 — on a shutdown signal, consumer-service completes acknowledgment
 * of in-flight work and the process exits within 10 seconds. Stops the listener CONTAINER
 * directly (which respects T053's 10-second {@code shutdownTimeout}) — the exact mechanism a real
 * SIGTERM triggers via Spring's context-close lifecycle — rather than closing the whole (shared,
 * test-context-cached) Spring context.
 */
class GracefulShutdownTest extends ConsumerServiceIntegrationTest {

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Test
    void shutdownCompletesWithinTenSecondsAfterProcessingInFlightWork() throws Exception {
        long consumedBefore = runtimeState.getMessagesConsumed();
        publishValidEnvelope(1, "before-shutdown");
        awaitCondition(() -> runtimeState.getMessagesConsumed() > consumedBefore, 10_000);
        assertThat(runtimeState.getMessagesConsumed()).isGreaterThan(consumedBefore);

        MessageListenerContainer container = registry.getListenerContainers().iterator().next();

        long start = System.nanoTime();
        container.stop();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs).isLessThan(10_000);
        assertThat(container.isRunning()).isFalse();

        // Leave the container running again for any later test sharing this context.
        container.start();
    }
}
