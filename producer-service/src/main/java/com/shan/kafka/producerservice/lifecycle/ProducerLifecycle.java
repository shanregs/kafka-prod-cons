package com.shan.kafka.producerservice.lifecycle;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Producer lifecycle state machine (data-model.md {@code ProducerLifecycleState}), implementing
 * research.md R2's install-then-activate/cancel protocol: {@link #start()} installs a new
 * {@link ProductionTask} via a single compare-and-set on {@code currentTask}, then activates it;
 * {@link #stop()} atomically clears the slot and cancels whatever task was there. Because
 * activation/cancellation on a given task are themselves a single mutually-exclusive transition
 * ({@link ProductionTask}), no task can ever become active once STOPPED has been reported,
 * regardless of call interleaving (FR-006–FR-008).
 */
@Component
public class ProducerLifecycle {

    public enum State { STOPPED, RUNNING }

    /** {@code error} is non-null only for the invalid-configuration case (Edge Cases). */
    public record StartResult(State state, Double configuredRate, String error) {
        public boolean isInvalid() {
            return error != null;
        }
    }

    private final Double configuredRate;
    private final ProductionTaskFactory taskFactory;

    private final AtomicReference<ProductionTask> currentTask = new AtomicReference<>();
    /** Never cleared on stop, so counters remain readable after STOPPED (data-model.md). */
    private volatile ProductionTask lastTask;
    private volatile Instant startedAt;

    @Autowired
    public ProducerLifecycle(
            @Value("${app.producer.rate-per-second:}") String configuredRateRaw,
            ProductionTaskFactory taskFactory) {
        this.configuredRate = parseRate(configuredRateRaw);
        this.taskFactory = taskFactory;
    }

    private static Double parseRate(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * STOPPED→RUNNING (or idempotent no-op if already RUNNING). Rejects an absent/zero/negative
     * configured rate without transitioning (data-model.md Validation rules, Edge Cases).
     */
    public StartResult start() {
        if (configuredRate == null || configuredRate <= 0) {
            return new StartResult(State.STOPPED, null, "invalid configured rate: must be > 0");
        }

        ProductionTask candidate = taskFactory.create(configuredRate);
        if (!currentTask.compareAndSet(null, candidate)) {
            // Someone else already installed and (is) activating a task; idempotent success.
            return new StartResult(State.RUNNING, configuredRate, null);
        }

        startedAt = Instant.now();
        lastTask = candidate;
        afterInstall(candidate);
        candidate.activate();
        return new StartResult(State.RUNNING, configuredRate, null);
    }

    /** RUNNING→STOPPED (or idempotent no-op if already STOPPED). */
    public void stop() {
        ProductionTask task = currentTask.getAndSet(null);
        if (task != null) {
            task.cancel();
        }
    }

    /**
     * FR-025/SC-011: stop issuing new messages on application shutdown, within 10 seconds. Spring
     * calls this automatically when the context closes (e.g. on SIGTERM) — without it, the
     * production task's non-daemon scheduler thread (research.md R2/{@link ProductionTask}) would
     * otherwise keep the JVM from exiting at all.
     */
    @PreDestroy
    public void shutdown() {
        stop();
    }

    public State getState() {
        return currentTask.get() != null ? State.RUNNING : State.STOPPED;
    }

    public Double getConfiguredRate() {
        return configuredRate;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public long getMessagesProduced() {
        return lastTask != null ? lastTask.getMessagesProduced() : 0;
    }

    public long getLastSequenceNumber() {
        return lastTask != null ? lastTask.getLastSequenceNumber() : 0;
    }

    public String getLastError() {
        return lastTask != null ? lastTask.getLastError() : null;
    }

    /** Test-only seam (research.md R2, T019): a no-op in production. */
    void afterInstall(ProductionTask installed) {
    }

    /** Test-only accessor (T019): whether the installed task, if any, is actually active. */
    boolean isProductionActive() {
        ProductionTask task = currentTask.get();
        return task != null && task.isActive();
    }
}
