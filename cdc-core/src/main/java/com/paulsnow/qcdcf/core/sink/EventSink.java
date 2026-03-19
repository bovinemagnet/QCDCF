package com.paulsnow.qcdcf.core.sink;

import com.paulsnow.qcdcf.model.ChangeEnvelope;

import java.util.List;

/**
 * Receives change events from the CDC pipeline for delivery to a downstream system.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public interface EventSink {

    /**
     * Publishes a single event.
     *
     * @param event the change event to publish
     * @return the result of the publication attempt
     */
    PublishResult publish(ChangeEnvelope event);

    /**
     * Publishes a batch of events.
     * <p>
     * Implementations may choose to process events individually or as an atomic batch.
     *
     * @param events the change events to publish
     * @return the result of the publication attempt
     */
    PublishResult publishBatch(List<ChangeEnvelope> events);
}
