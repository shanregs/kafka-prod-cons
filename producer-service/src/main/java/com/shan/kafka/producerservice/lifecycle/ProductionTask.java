package com.shan.kafka.producerservice.lifecycle;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * The install-then-activate/cancel protocol's other half (research.md R2, alongside
 * {@link ProducerLifecycle}'s compare-and-set install/clear). A task is built inert; only
 * {@link #activate()} actually invokes {@code scheduler}, and only the first of
 * {@link #activate()}/{@link #cancel()} to run has any effect — the loser is a no-op. This is what
 * guarantees a task can never start producing after the lifecycle has already reported STOPPED,
 * regardless of how {@code /startmsg}/{@code /stopmsg} calls interleave.
 */
class ProductionTask {

    private enum Phase { PENDING, ACTIVE, CANCELLED }

    private final AtomicReference<Phase> phase = new AtomicReference<>(Phase.PENDING);
    private final Supplier<AutoCloseable> scheduler;
    private volatile AutoCloseable scheduledHandle;

    ProductionTask(Supplier<AutoCloseable> scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * @return {@code true} if this call performed the activation (cancel had not already won);
     *         {@code false} if the task was already cancelled — a no-op, nothing is scheduled.
     */
    boolean activate() {
        if (phase.compareAndSet(Phase.PENDING, Phase.ACTIVE)) {
            scheduledHandle = scheduler.get();
            return true;
        }
        return false;
    }

    /**
     * Idempotent. If {@link #activate()} hasn't run yet, marks the task so a later call to it
     * becomes a no-op. If already active, stops the scheduled work.
     */
    void cancel() {
        Phase previous = phase.getAndSet(Phase.CANCELLED);
        if (previous == Phase.ACTIVE && scheduledHandle != null) {
            try {
                scheduledHandle.close();
            } catch (Exception ignored) {
                // Best-effort cancellation; nothing meaningful to do if the scheduler's own
                // shutdown hook fails.
            }
        }
    }

    boolean isActive() {
        return phase.get() == Phase.ACTIVE;
    }

    // --- Run-scoped counters (data-model.md ProducerLifecycleState), owned by this task since a
    // task IS one run. T031's peek-then-commit-only-on-success algorithm: a tick peeks the next
    // number, builds+sends the envelope with it, and only commits (advancing the counter) after a
    // confirmed successful send — so a failed/timed-out send never consumes a number (no gaps),
    // and the next tick reuses the same one. ---

    private final AtomicLong sequenceCounter = new AtomicLong();
    private final AtomicLong messagesProduced = new AtomicLong();
    private volatile String lastError;

    /** What the next successfully-sent message's sequence number would be. Does not advance it. */
    long peekNextSequenceNumber() {
        return sequenceCounter.get() + 1;
    }

    /** Call only after the send for {@code sequenceNumber} is confirmed successful. */
    void commitSuccessfulSend(long sequenceNumber) {
        sequenceCounter.set(sequenceNumber);
        messagesProduced.incrementAndGet();
    }

    void recordSendFailure(String error) {
        lastError = error;
    }

    long getMessagesProduced() {
        return messagesProduced.get();
    }

    long getLastSequenceNumber() {
        return sequenceCounter.get();
    }

    String getLastError() {
        return lastError;
    }
}
