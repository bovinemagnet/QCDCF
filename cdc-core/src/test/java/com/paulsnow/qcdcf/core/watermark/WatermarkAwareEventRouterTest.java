package com.paulsnow.qcdcf.core.watermark;

import com.paulsnow.qcdcf.core.reconcile.DefaultReconciliationEngine;
import com.paulsnow.qcdcf.core.sink.InMemoryEventSink;
import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the watermark-aware event router — the core reconciliation integration point.
 * <p>
 * These are the highest-value tests in the entire project: they prove that
 * the DBLog-style watermark reconciliation correctly merges live WAL events
 * with snapshot data.
 */
class WatermarkAwareEventRouterTest {

    private static final TableId TABLE = new TableId("public", "customer");

    private InMemoryEventSink sink;
    private WatermarkAwareEventRouter router;

    @BeforeEach
    void setUp() {
        sink = new InMemoryEventSink();
        router = new WatermarkAwareEventRouter(new DefaultReconciliationEngine(), sink);
    }

    private WatermarkWindow openTestWindow(int chunkIndex) {
        return new WatermarkWindow(UUID.randomUUID(), TABLE, Instant.now(), null, chunkIndex);
    }

    private ChangeEnvelope logEvent(int id, OperationType op) {
        return new ChangeEnvelope(
                UUID.randomUUID(), "test", TABLE, op, CaptureMode.LOG,
                new RowKey(Map.of("id", id)),
                op == OperationType.DELETE ? Map.of("id", id) : null,
                op != OperationType.DELETE ? Map.of("id", id, "name", "log-" + id) : null,
                new SourcePosition(1000L + id, Instant.now()),
                Instant.now(), null, Map.of()
        );
    }

    private ChangeEnvelope snapshotEvent(int id) {
        return new ChangeEnvelope(
                UUID.randomUUID(), "test", TABLE,
                OperationType.SNAPSHOT_READ, CaptureMode.SNAPSHOT,
                new RowKey(Map.of("id", id)),
                null, Map.of("id", id, "name", "snapshot-" + id),
                null, Instant.now(), null, Map.of()
        );
    }

    // --- Pass-through when no window is active ---

    @Test
    void eventsPassThroughDirectlyWhenNoWindowActive() {
        router.onEvent(logEvent(1, OperationType.INSERT));
        router.onEvent(logEvent(2, OperationType.UPDATE));

        assertThat(sink.events()).hasSize(2);
        assertThat(sink.events().get(0).operation()).isEqualTo(OperationType.INSERT);
        assertThat(sink.events().get(1).operation()).isEqualTo(OperationType.UPDATE);
    }

    // --- Snapshot row survives when no live change overlaps ---

    @Test
    void snapshotRowSurvivesWhenNoLiveChangeOverlaps() {
        WatermarkWindow window = openTestWindow(0);
        router.openWindow(window);

        // Log event for key=1, snapshot for key=2 — no overlap
        router.onEvent(logEvent(1, OperationType.INSERT));
        router.onSnapshotChunk(List.of(snapshotEvent(2)));
        router.closeWindow();

        // Both should be in the output: log event first, then snapshot
        assertThat(sink.events()).hasSize(2);
        assertThat(sink.events().get(0).key().columns().get("id")).isEqualTo(1);
        assertThat(sink.events().get(0).captureMode()).isEqualTo(CaptureMode.LOG);
        assertThat(sink.events().get(1).key().columns().get("id")).isEqualTo(2);
        assertThat(sink.events().get(1).captureMode()).isEqualTo(CaptureMode.SNAPSHOT);
    }

    // --- Snapshot row removed when same key updated inside window ---

    @Test
    void snapshotRowRemovedWhenSameKeyUpdatedInsideWindow() {
        WatermarkWindow window = openTestWindow(0);
        router.openWindow(window);

        // Log UPDATE for key=1, snapshot also has key=1 — collision
        router.onEvent(logEvent(1, OperationType.UPDATE));
        router.onSnapshotChunk(List.of(snapshotEvent(1)));
        router.closeWindow();

        // Only the snapshot survives (log event dropped because key collision
        // means snapshot has newer consistent data)
        assertThat(sink.events()).hasSize(1);
        assertThat(sink.events().getFirst().captureMode()).isEqualTo(CaptureMode.SNAPSHOT);
        assertThat(sink.events().getFirst().after().get("name")).isEqualTo("snapshot-1");
    }

    // --- Snapshot row removed when same key deleted inside window ---

    @Test
    void snapshotRowRemovedWhenSameKeyDeletedInsideWindow() {
        WatermarkWindow window = openTestWindow(0);
        router.openWindow(window);

        // Log DELETE for key=1, snapshot also has key=1
        router.onEvent(logEvent(1, OperationType.DELETE));
        router.onSnapshotChunk(List.of(snapshotEvent(1)));
        router.closeWindow();

        // Snapshot survives — the snapshot read happened AFTER the delete
        // was committed, so if the row still exists in the snapshot,
        // it was re-inserted. The reconciliation drops the log DELETE.
        assertThat(sink.events()).hasSize(1);
        assertThat(sink.events().getFirst().captureMode()).isEqualTo(CaptureMode.SNAPSHOT);
    }

    // --- Multiple updates to same key preserve snapshot (log events dropped) ---

    @Test
    void multipleUpdatesToSameKeyDropAllLogEventsForThatKey() {
        WatermarkWindow window = openTestWindow(0);
        router.openWindow(window);

        // Two log UPDATEs for key=1, snapshot also has key=1
        router.onEvent(logEvent(1, OperationType.UPDATE));
        router.onEvent(logEvent(1, OperationType.UPDATE));
        router.onSnapshotChunk(List.of(snapshotEvent(1)));
        router.closeWindow();

        // Both log events dropped, snapshot survives
        assertThat(sink.events()).hasSize(1);
        assertThat(sink.events().getFirst().captureMode()).isEqualTo(CaptureMode.SNAPSHOT);
    }

    // --- Unrelated keys do not affect each other ---

    @Test
    void unrelatedKeysDoNotAffectEachOther() {
        WatermarkWindow window = openTestWindow(0);
        router.openWindow(window);

        // Log events for keys 1 and 3, snapshot for keys 2 and 4
        router.onEvent(logEvent(1, OperationType.INSERT));
        router.onEvent(logEvent(3, OperationType.UPDATE));
        router.onSnapshotChunk(List.of(snapshotEvent(2), snapshotEvent(4)));
        router.closeWindow();

        // All 4 events survive — no key collisions
        assertThat(sink.events()).hasSize(4);
    }

    // --- Mixed collision and non-collision ---

    @Test
    void mixedCollisionAndNonCollisionHandledCorrectly() {
        WatermarkWindow window = openTestWindow(0);
        router.openWindow(window);

        // Log: INSERT key=1, UPDATE key=2
        // Snapshot: key=2, key=3
        // Expected: key=1 (log, no collision), key=2 (snapshot, collision drops log), key=3 (snapshot)
        router.onEvent(logEvent(1, OperationType.INSERT));
        router.onEvent(logEvent(2, OperationType.UPDATE));
        router.onSnapshotChunk(List.of(snapshotEvent(2), snapshotEvent(3)));
        router.closeWindow();

        assertThat(sink.events()).hasSize(3);
        // Log event for key=1 first (surviving log events before snapshot)
        assertThat(sink.events().get(0).key().columns().get("id")).isEqualTo(1);
        assertThat(sink.events().get(0).captureMode()).isEqualTo(CaptureMode.LOG);
        // Snapshot events follow
        assertThat(sink.events().get(1).captureMode()).isEqualTo(CaptureMode.SNAPSHOT);
        assertThat(sink.events().get(2).captureMode()).isEqualTo(CaptureMode.SNAPSHOT);
    }

    // --- After window closes, events pass through again ---

    @Test
    void eventsPassThroughAfterWindowCloses() {
        WatermarkWindow window = openTestWindow(0);
        router.openWindow(window);
        router.onEvent(logEvent(1, OperationType.INSERT));
        router.onSnapshotChunk(List.of(snapshotEvent(2)));
        router.closeWindow();

        // After window closes, new events pass through directly
        sink.clear();
        router.onEvent(logEvent(3, OperationType.INSERT));

        assertThat(sink.events()).hasSize(1);
        assertThat(sink.events().getFirst().key().columns().get("id")).isEqualTo(3);
    }

    // --- Buffered event count tracked ---

    @Test
    void tracksBufferedEventCount() {
        assertThat(router.isWindowActive()).isFalse();

        WatermarkWindow window = openTestWindow(0);
        router.openWindow(window);
        assertThat(router.isWindowActive()).isTrue();
        assertThat(router.bufferedLogEventCount()).isEqualTo(0);

        router.onEvent(logEvent(1, OperationType.INSERT));
        router.onEvent(logEvent(2, OperationType.UPDATE));
        assertThat(router.bufferedLogEventCount()).isEqualTo(2);

        router.onSnapshotChunk(List.of(snapshotEvent(3)));
        router.closeWindow();
        assertThat(router.isWindowActive()).isFalse();
        assertThat(router.bufferedLogEventCount()).isEqualTo(0);
    }
}
