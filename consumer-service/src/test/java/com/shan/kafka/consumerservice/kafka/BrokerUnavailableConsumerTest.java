package com.shan.kafka.consumerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.DisconnectException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import com.shan.kafka.consumerservice.support.ConsumerServiceIntegrationTest;

/**
 * T047 (US4): FR-024 — rather than physically stopping the embedded broker (unreliable timing),
 * deterministically invokes T013/T039's real, wired {@link CommonErrorHandler} bean with a
 * simulated connectivity exception ({@link DisconnectException}, harmless {@code Consumer}/
 * {@code MessageListenerContainer} doubles standing in for the ones the container would normally
 * pass). Asserts the consumer doesn't crash, surfaces the condition via {@code lastError}, and —
 * since nothing was actually broken — keeps consuming normally afterward.
 */
class BrokerUnavailableConsumerTest extends ConsumerServiceIntegrationTest {

    @Autowired
    private CommonErrorHandler errorHandler;

    @Test
    void connectivityErrorSurfacesViaLastErrorWithoutCrashingAndConsumptionContinues() throws Exception {
        assertThat(runtimeState.getLastError()).isNull();

        Consumer<?, ?> consumerDouble = mock(Consumer.class);
        MessageListenerContainer containerDouble = mock(MessageListenerContainer.class);

        assertThatCode(() -> errorHandler.handleOtherException(
                new DisconnectException("simulated broker unavailable"),
                consumerDouble, containerDouble, false))
                .doesNotThrowAnyException();

        assertThat(runtimeState.getLastError()).isNotBlank();

        long consumedBefore = runtimeState.getMessagesConsumed();
        publishValidEnvelope(1, "after-simulated-connectivity-error");
        awaitCondition(() -> runtimeState.getMessagesConsumed() > consumedBefore, 10_000);
        assertThat(runtimeState.getMessagesConsumed()).isGreaterThan(consumedBefore);
    }
}
