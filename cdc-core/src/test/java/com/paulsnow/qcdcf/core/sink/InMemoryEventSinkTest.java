package com.paulsnow.qcdcf.core.sink;

import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryEventSinkTest {

    private InMemoryEventSink sink;

    @BeforeEach
    void setUp() {
        sink = new InMemoryEventSink();
    }

    private ChangeEnvelope testEvent() {
        return new ChangeEnvelope(
                UUID.randomUUID(), "test",
                new TableId("public", "customer"),
                OperationType.INSERT, CaptureMode.LOG,
                new RowKey(Map.of("id", 1)),
                null, Map.of("name", "Alice"),
                new SourcePosition(100L, Instant.now()),
                Instant.now(), null, Map.of()
        );
    }

    @Test
    void publishStoresEvent() {
        var event = testEvent();
        var result = sink.publish(event);

        assertThat(result.isSuccess()).isTrue();
        assertThat(sink.events()).containsExactly(event);
    }

    @Test
    void publishBatchStoresAllEvents() {
        var events = List.of(testEvent(), testEvent());
        var result = sink.publishBatch(events);

        assertThat(result).isInstanceOf(PublishResult.Success.class);
        assertThat(((PublishResult.Success) result).eventCount()).isEqualTo(2);
        assertThat(sink.events()).hasSize(2);
    }

    @Test
    void clearRemovesAllEvents() {
        sink.publish(testEvent());
        sink.clear();

        assertThat(sink.events()).isEmpty();
    }

    @Test
    void eventsListIsUnmodifiable() {
        sink.publish(testEvent());
        var events = sink.events();

        org.junit.jupiter.api.Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> events.add(testEvent())
        );
    }
}
