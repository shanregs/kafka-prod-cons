package com.shan.kafka.producerservice.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T018 (US1): FR-008, SC-003 — firing several simultaneous POST /startmsg calls results in exactly
 * one active production loop and a consistently RUNNING reported state. "Exactly one loop" is
 * verified indirectly here (via HTTP, so it can't observe the internal task instance directly —
 * that's what T019's white-box race test is for) by bounding the observed message rate well below
 * what two concurrent loops at the configured rate would produce.
 */
class ConcurrentStartTest extends ProducerServiceIntegrationTest {

    private static final int CONCURRENT_CALLS = 10;
    private static final long CONFIGURED_RATE_PER_SECOND = 5; // matches the base's test property

    private Consumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void concurrentStartCallsProduceExactlyOneActiveLoop() throws Exception {
        consumer = createTopicConsumer("concurrent-start");

        CountDownLatch readyToFire = new CountDownLatch(CONCURRENT_CALLS);
        CountDownLatch fire = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_CALLS);
        try {
            List<Callable<ResponseEntity<?>>> calls = java.util.stream.IntStream
                    .range(0, CONCURRENT_CALLS)
                    .<Callable<ResponseEntity<?>>>mapToObj(i -> () -> {
                        readyToFire.countDown();
                        fire.await(5, TimeUnit.SECONDS);
                        return startProducerResponse();
                    })
                    .toList();

            List<Future<ResponseEntity<?>>> futures = executor.invokeAll(calls, 10, TimeUnit.SECONDS);
            readyToFire.await(5, TimeUnit.SECONDS);
            fire.countDown();

            for (Future<ResponseEntity<?>> future : futures) {
                ResponseEntity<?> response = future.get(10, TimeUnit.SECONDS);
                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                assertThat(response.getBody()).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
                        .containsEntry("state", "RUNNING");
            }
        } finally {
            executor.shutdownNow();
        }

        // A single loop at the configured rate produces ~ rate * seconds messages. Two loops
        // (the bug this guards against) would produce roughly double. Use a generous ceiling
        // that still clearly separates "one loop" from "two loops".
        Duration window = Duration.ofSeconds(4);
        int observed = countRecords(consumer, window);
        long singleLoopCeiling = (long) (CONFIGURED_RATE_PER_SECOND * window.toSeconds() * 1.5);
        assertThat(observed).isLessThanOrEqualTo((int) singleLoopCeiling);
    }
}
