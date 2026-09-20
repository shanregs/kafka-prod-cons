package com.shan.kafka.producerservice.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/**
 * T019 (US1): a deterministic, white-box test of research.md R2's install-then-activate protocol,
 * exercised directly against {@code ProducerLifecycle}/{@code ProductionTask} rather than through
 * HTTP (which cannot reliably force this exact interleaving). This test file — and the contract it
 * assumes — is written before those classes exist (constitution Principle III); T021/T022 (next
 * phase) must implement to this exact contract:
 *
 * <ul>
 *   <li>{@code ProductionTask(Supplier<AutoCloseable> scheduler)}: {@code activate()} performs a
 *       one-time PENDING→ACTIVE transition, calling {@code scheduler.get()} and returning
 *       {@code true} only if it won that transition (cancel had not already run); otherwise it's a
 *       no-op returning {@code false}. {@code cancel()} performs a one-time transition to
 *       CANCELLED and is idempotent; if the task was already ACTIVE it closes the handle
 *       {@code scheduler.get()} returned. {@code isActive()} reports whether it's currently
 *       ACTIVE.</li>
 *   <li>{@code ProducerLifecycle(String configuredRateRaw, ProductionTaskFactory taskFactory)}:
 *       {@code start()} builds a new task via the factory, installs it with one compare-and-set
 *       into a shared slot, calls the package-private, no-op-by-default seam
 *       {@code afterInstall(ProductionTask)}, then calls {@code task.activate()}. {@code stop()}
 *       atomically clears the slot and calls {@code cancel()} on whatever task was there.
 *       {@code isProductionActive()} (package-private) reports whether the currently-installed
 *       task, if any, is active.</li>
 *   <li>{@code ProductionTaskFactory}: a {@code @FunctionalInterface} with
 *       {@code ProductionTask create(double rate)}.</li>
 * </ul>
 */
class StartStopRaceTest {

    // --- (a) explicit-ordering sub-test: no real concurrency, just the two possible orderings ---

    @Test
    void activateIsANoOpWhenCancelHasAlreadyWon() {
        AtomicInteger scheduledCount = new AtomicInteger();
        ProductionTask task = new ProductionTask(() -> {
            scheduledCount.incrementAndGet();
            AutoCloseable handle = () -> { };
            return handle;
        });

        task.cancel();
        boolean activated = task.activate();

        assertThat(activated).isFalse();
        assertThat(task.isActive()).isFalse();
        assertThat(scheduledCount.get()).isZero();
    }

    @Test
    void cancelStopsAnAlreadyActiveTask() {
        AtomicInteger closedCount = new AtomicInteger();
        ProductionTask task = new ProductionTask(() -> {
            AutoCloseable handle = closedCount::incrementAndGet;
            return handle;
        });

        boolean activated = task.activate();
        assertThat(activated).isTrue();
        assertThat(task.isActive()).isTrue();

        task.cancel();

        assertThat(task.isActive()).isFalse();
        assertThat(closedCount.get()).isEqualTo(1);
    }

    // --- (b) concurrent-thread confirmation: reproduces R2's exact race using a barrier ---

    @Test
    void concurrentStopWinningBeforeLateActivateLeavesNoActiveProduction() throws Exception {
        RaceTestableProducerLifecycle lifecycle = new RaceTestableProducerLifecycle("5");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> startCall = executor.submit(lifecycle::start);

            // Wait until start() has installed its task and is paused just before activating it —
            // i.e., exactly the window the naive "CAS-then-schedule" design left open.
            assertThat(lifecycle.installedSignal.await(5, TimeUnit.SECONDS)).isTrue();

            // A concurrent stop() must win the clear-and-cancel while nothing has activated yet.
            lifecycle.stop();

            // Only now let start()'s deferred activate() call proceed; it must be a no-op.
            lifecycle.releaseActivate.countDown();
            startCall.get(5, TimeUnit.SECONDS);

            assertThat(lifecycle.getState()).isEqualTo(ProducerLifecycle.State.STOPPED);
            assertThat(lifecycle.isProductionActive()).isFalse();
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test-only subclass exposing R2's install/activate seam as a controllable barrier. */
    private static final class RaceTestableProducerLifecycle extends ProducerLifecycle {

        final CountDownLatch installedSignal = new CountDownLatch(1);
        final CountDownLatch releaseActivate = new CountDownLatch(1);

        RaceTestableProducerLifecycle(String configuredRateRaw) {
            super(configuredRateRaw, rate -> {
                AutoCloseable handle = () -> { };
                return new ProductionTask(() -> handle);
            });
        }

        @Override
        void afterInstall(ProductionTask installed) {
            installedSignal.countDown();
            try {
                if (!releaseActivate.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("releaseActivate latch timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
