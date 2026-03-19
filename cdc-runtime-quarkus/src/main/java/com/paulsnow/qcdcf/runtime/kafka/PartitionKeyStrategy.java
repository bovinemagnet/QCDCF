package com.paulsnow.qcdcf.runtime.kafka;

import com.paulsnow.qcdcf.model.ChangeEnvelope;

/**
 * Determines the Kafka partition key for a change event.
 * <p>
 * Uses the canonical row key to ensure events for the same row go to the same partition.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PartitionKeyStrategy {

    /**
     * Computes the partition key for the given event.
     *
     * @param event the change event
     * @return a deterministic partition key string
     */
    public String partitionKey(ChangeEnvelope event) {
        return event.tableId().canonicalName() + ":" + event.key().toCanonical();
    }
}
