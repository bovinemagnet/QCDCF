package com.paulsnow.qcdcf.core.sink;

import com.paulsnow.qcdcf.model.ChangeEnvelope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An in-memory event sink that stores events in a list. Intended for testing.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class InMemoryEventSink implements EventSink {

    private final List<ChangeEnvelope> events = new ArrayList<>();

    @Override
    public PublishResult publish(ChangeEnvelope event) {
        events.add(event);
        return new PublishResult.Success(1);
    }

    @Override
    public PublishResult publishBatch(List<ChangeEnvelope> batch) {
        events.addAll(batch);
        return new PublishResult.Success(batch.size());
    }

    /** Returns an unmodifiable view of all captured events. */
    public List<ChangeEnvelope> events() {
        return Collections.unmodifiableList(events);
    }

    /** Clears all captured events. */
    public void clear() {
        events.clear();
    }
}
