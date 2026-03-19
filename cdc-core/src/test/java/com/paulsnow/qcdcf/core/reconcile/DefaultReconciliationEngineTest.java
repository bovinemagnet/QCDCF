package com.paulsnow.qcdcf.core.reconcile;

import com.paulsnow.qcdcf.model.*;
import com.paulsnow.qcdcf.core.watermark.WatermarkWindow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultReconciliationEngineTest {

    private static final TableId TABLE = new TableId("public", "customer");
    private DefaultReconciliationEngine engine;
    private WatermarkWindow window;

    @BeforeEach
    void setUp() {
        engine = new DefaultReconciliationEngine();
        window = new WatermarkWindow(
                UUID.randomUUID(), TABLE,
                Instant.now(), Instant.now().plusSeconds(1), 0
        );
    }

    private ChangeEnvelope logEvent(int id) {
        return new ChangeEnvelope(
                UUID.randomUUID(), "test", TABLE,
                OperationType.UPDATE, CaptureMode.LOG,
                new RowKey(Map.of("id", id)),
                Map.of("name", "old"), Map.of("name", "new"),
                new SourcePosition(100L + id, Instant.now()),
                Instant.now(), null, Map.of()
        );
    }

    private ChangeEnvelope snapshotEvent(int id) {
        return new ChangeEnvelope(
                UUID.randomUUID(), "test", TABLE,
                OperationType.SNAPSHOT_READ, CaptureMode.SNAPSHOT,
                new RowKey(Map.of("id", id)),
                null, Map.of("name", "snapshot"),
                null, Instant.now(), null, Map.of()
        );
    }

    @Test
    void noCollisionsKeepsAllEvents() {
        var log = List.of(logEvent(1), logEvent(2));
        var snapshot = List.of(snapshotEvent(3), snapshotEvent(4));

        var result = engine.reconcile(window, log, snapshot);

        assertThat(result.mergedEvents()).hasSize(4);
        assertThat(result.logEventsDropped()).isZero();
        assertThat(result.logEvents()).isEqualTo(2);
        assertThat(result.snapshotEvents()).isEqualTo(2);
    }

    @Test
    void collidingKeyDropsLogEvent() {
        var log = List.of(logEvent(1), logEvent(2));
        var snapshot = List.of(snapshotEvent(1));  // collides with logEvent(1)

        var result = engine.reconcile(window, log, snapshot);

        assertThat(result.mergedEvents()).hasSize(2);  // logEvent(2) + snapshotEvent(1)
        assertThat(result.logEventsDropped()).isEqualTo(1);
    }

    @Test
    void allCollidingDropsAllLogEvents() {
        var log = List.of(logEvent(1), logEvent(2));
        var snapshot = List.of(snapshotEvent(1), snapshotEvent(2));

        var result = engine.reconcile(window, log, snapshot);

        assertThat(result.mergedEvents()).hasSize(2);  // only snapshot events
        assertThat(result.logEventsDropped()).isEqualTo(2);
        // All remaining events should be snapshot events
        assertThat(result.mergedEvents()).allMatch(e -> e.captureMode() == CaptureMode.SNAPSHOT);
    }

    @Test
    void emptyLogEventsReturnsOnlySnapshotEvents() {
        var snapshot = List.of(snapshotEvent(1), snapshotEvent(2));

        var result = engine.reconcile(window, List.of(), snapshot);

        assertThat(result.mergedEvents()).hasSize(2);
        assertThat(result.logEventsDropped()).isZero();
    }

    @Test
    void emptySnapshotEventsReturnsOnlyLogEvents() {
        var log = List.of(logEvent(1), logEvent(2));

        var result = engine.reconcile(window, log, List.of());

        assertThat(result.mergedEvents()).hasSize(2);
        assertThat(result.logEventsDropped()).isZero();
    }

    @Test
    void bothEmptyReturnsEmptyResult() {
        var result = engine.reconcile(window, List.of(), List.of());

        assertThat(result.mergedEvents()).isEmpty();
        assertThat(result.logEventsDropped()).isZero();
    }

    @Test
    void survivingLogEventsAppearBeforeSnapshotEvents() {
        var log = List.of(logEvent(1), logEvent(3));
        var snapshot = List.of(snapshotEvent(1), snapshotEvent(2));  // collides with logEvent(1)

        var result = engine.reconcile(window, log, snapshot);

        assertThat(result.mergedEvents()).hasSize(3);
        // First event should be the surviving log event (key=3)
        assertThat(result.mergedEvents().get(0).captureMode()).isEqualTo(CaptureMode.LOG);
        assertThat(result.mergedEvents().get(0).key().toCanonical()).isEqualTo("id=3");
        // Remaining should be snapshot events
        assertThat(result.mergedEvents().get(1).captureMode()).isEqualTo(CaptureMode.SNAPSHOT);
        assertThat(result.mergedEvents().get(2).captureMode()).isEqualTo(CaptureMode.SNAPSHOT);
    }
}
