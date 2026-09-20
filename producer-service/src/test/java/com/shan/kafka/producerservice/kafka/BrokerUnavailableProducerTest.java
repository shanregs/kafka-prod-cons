package com.shan.kafka.producerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;

import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T046 (US4): FR-023, SC-007 — rather than physically stopping the embedded broker (unreliable
 * timing), substitutes a deterministic fault-injecting double for T012's injectable
 * {@link KafkaTemplate} bean, toggled to fail every send and later toggled back. Asserts
 * {@code GET /status} always returns 200 with a well-defined state and a non-null lastError while
 * failing, and that production resumes once the double is toggled back to succeeding.
 */
class BrokerUnavailableProducerTest extends ProducerServiceIntegrationTest {

    @Autowired
    private ToggleableKafkaTemplate toggleableKafkaTemplate;

    @Test
    void statusStaysWellDefinedWhileFailingAndProductionResumesAfterRecovery() throws Exception {
        toggleableKafkaTemplate.setFailing(true);
        startProducer();

        awaitLastErrorPresent();

        ResponseEntity<Map> whileFailing = restTemplate.getForEntity("/status", Map.class);
        assertThat(whileFailing.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<?, ?> failingBody = whileFailing.getBody();
        assertThat(failingBody.get("state")).isEqualTo("RUNNING"); // well-defined, not corrupted
        assertThat(failingBody.get("lastError")).isNotNull();

        toggleableKafkaTemplate.setFailing(false);

        long deadline = System.currentTimeMillis() + 10_000;
        long produced = 0;
        while (System.currentTimeMillis() < deadline) {
            Map<?, ?> body = restTemplate.getForEntity("/status", Map.class).getBody();
            produced = ((Number) body.get("messagesProduced")).longValue();
            if (produced > 0) {
                break;
            }
            Thread.sleep(200);
        }
        assertThat(produced).isGreaterThan(0);
    }

    private void awaitLastErrorPresent() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            Map<?, ?> body = restTemplate.getForEntity("/status", Map.class).getBody();
            if (body.get("lastError") != null) {
                return;
            }
            Thread.sleep(200);
        }
    }

    @TestConfiguration
    static class FaultInjectionConfig {

        @Bean
        @Primary
        ToggleableKafkaTemplate toggleableKafkaTemplate(
                ProducerFactory<String, KafkaMessageEnvelope> producerFactory) {
            return new ToggleableKafkaTemplate(producerFactory);
        }
    }

    /** Delegates to a real producer factory but can be told to fail every send on demand. */
    static class ToggleableKafkaTemplate extends KafkaTemplate<String, KafkaMessageEnvelope> {

        private volatile boolean failing;

        ToggleableKafkaTemplate(ProducerFactory<String, KafkaMessageEnvelope> producerFactory) {
            super(producerFactory);
        }

        void setFailing(boolean failing) {
            this.failing = failing;
        }

        @Override
        public CompletableFuture<SendResult<String, KafkaMessageEnvelope>> send(String topic, KafkaMessageEnvelope data) {
            if (failing) {
                CompletableFuture<SendResult<String, KafkaMessageEnvelope>> future = new CompletableFuture<>();
                future.completeExceptionally(new TimeoutException("simulated broker unavailable"));
                return future;
            }
            return super.send(topic, data);
        }
    }
}
