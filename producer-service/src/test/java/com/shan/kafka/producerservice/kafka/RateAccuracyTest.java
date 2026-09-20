package com.shan.kafka.producerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T026 (US2): FR-016, FR-017, SC-004 — over a 60-second window, the aggregate produced count is
 * within 10% of the configured rate (5 msg/s, per the shared test base). This is the real SC-004
 * window, not a shortened stand-in, so this test takes ~60s to run.
 */
class RateAccuracyTest extends ProducerServiceIntegrationTest {

    private static final long CONFIGURED_RATE_PER_SECOND = 5; // matches the base's test property
    private static final Duration WINDOW = Duration.ofSeconds(60);

    private Consumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void aggregateRateStaysWithinTenPercentOverSixtySeconds() {
        consumer = createTopicConsumer("rate-accuracy");

        startProducer();
        int observed = countRecords(consumer, WINDOW);
        stopProducer();

        long expected = CONFIGURED_RATE_PER_SECOND * WINDOW.toSeconds();
        long tolerance = Math.round(expected * 0.10);

        assertThat(observed)
                .isBetween((int) (expected - tolerance), (int) (expected + tolerance));
    }
}
