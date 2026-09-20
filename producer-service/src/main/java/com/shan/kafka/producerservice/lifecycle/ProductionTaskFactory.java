package com.shan.kafka.producerservice.lifecycle;

/**
 * Builds an inert (not-yet-scheduled) {@link ProductionTask} for a given configured rate. The
 * returned task must not begin producing until {@link ProductionTask#activate()} is called
 * (research.md R2's install-then-activate protocol).
 */
@FunctionalInterface
interface ProductionTaskFactory {

    ProductionTask create(double rate);
}
