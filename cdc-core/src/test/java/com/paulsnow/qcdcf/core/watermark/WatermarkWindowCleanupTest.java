package com.paulsnow.qcdcf.core.watermark;

import com.paulsnow.qcdcf.core.reconcile.DefaultReconciliationEngine;
import com.paulsnow.qcdcf.core.sink.InMemoryEventSink;
import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WatermarkWindowCleanupTest {

    private InMemoryEventSink sink;
    private WatermarkAwareEventRouter router;

    @BeforeEach
    void setUp() {
        sink = new InMemoryEventSink();
        router = new WatermarkAwareEventRouter(new DefaultReconciliationEngine(), sink);
    }

    @Test
    void cancelWindowClearsBufferedEvents() {
        var window = new WatermarkWindow(UUID.randomUUID(), new TableId("public", "customer"),
                Instant.now(), null, 0);

        router.openWindow(window);

        var logEvent = new ChangeEnvelope(
                UUID.randomUUID(), "test", new TableId("public", "customer"),
                OperationType.INSERT, CaptureMode.LOG,
                new RowKey(Map.of("id", 1)), null, Map.of("id", 1, "name", "Alice"),
                new SourcePosition(1000L, Instant.now()), Instant.now(), null, Map.of());
        router.onEvent(logEvent);

        router.cancelWindow(window);
        assertThat(sink.events()).isEmpty();
    }

    @Test
    void cancelWindowAllowsNewWindowToOpen() {
        var window1 = new WatermarkWindow(UUID.randomUUID(), new TableId("public", "customer"),
                Instant.now(), null, 0);

        router.openWindow(window1);
        router.cancelWindow(window1);

        var window2 = new WatermarkWindow(UUID.randomUUID(), new TableId("public", "customer"),
                Instant.now(), null, 1);
        router.openWindow(window2);
    }
}
