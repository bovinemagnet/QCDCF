package com.paulsnow.qcdcf.core.snapshot;

import com.paulsnow.qcdcf.core.watermark.WatermarkCoordinator;
import com.paulsnow.qcdcf.core.watermark.WatermarkWindow;
import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultSnapshotCoordinatorTest {

    private static final TableId TABLE = new TableId("public", "customer");

    @Test
    void readsAllChunksUntilExhausted() {
        // Mock watermark coordinator that just tracks window opens/closes
        var windows = new ArrayList<WatermarkWindow>();
        WatermarkCoordinator watermarkCoord = new WatermarkCoordinator() {
            @Override
            public WatermarkWindow openWindow(TableId tableId, int chunkIndex) {
                var w = new WatermarkWindow(UUID.randomUUID(), tableId, Instant.now(), null, chunkIndex);
                windows.add(w);
                return w;
            }
            @Override
            public WatermarkWindow closeWindow(WatermarkWindow window) {
                return window.close(Instant.now());
            }
            @Override
            public void cancelWindow(WatermarkWindow window) { /* no-op in test */ }
        };

        // Mock chunk reader: returns 3 rows for chunk 0, 2 rows for chunk 1 (less than chunk size = done)
        DefaultSnapshotCoordinator.ChunkReader reader = plan -> {
            int rowCount = plan.chunkIndex() == 0 ? 3 : 2;
            List<ChangeEnvelope> events = new ArrayList<>();
            for (int i = 0; i < rowCount; i++) {
                int id = plan.chunkIndex() * 3 + i + 1;
                events.add(new ChangeEnvelope(
                        UUID.randomUUID(), "test", TABLE,
                        OperationType.SNAPSHOT_READ, CaptureMode.SNAPSHOT,
                        new RowKey(Map.of("id", id)),
                        null, Map.of("id", id, "name", "row-" + id),
                        null, Instant.now(), null, Map.of()
                ));
            }
            RowKey lastKey = new RowKey(Map.of("id", plan.chunkIndex() * 3 + rowCount));
            return new DefaultSnapshotCoordinator.ChunkReader.ChunkReadResult(
                    events,
                    new SnapshotChunkResult(plan.chunkIndex(), rowCount, lastKey, Duration.ofMillis(10))
            );
        };

        // Collect chunks
        var allChunks = new ArrayList<List<ChangeEnvelope>>();
        SnapshotCoordinator.ChunkHandler handler = (events, result) -> allChunks.add(events);

        var coordinator = new DefaultSnapshotCoordinator(
                watermarkCoord, new DefaultChunkPlanner(), reader, handler);

        long total = coordinator.triggerSnapshot(TABLE, new SnapshotOptions(TABLE, 3));

        assertThat(total).isEqualTo(5);
        assertThat(allChunks).hasSize(2);
        assertThat(allChunks.get(0)).hasSize(3);
        assertThat(allChunks.get(1)).hasSize(2);
        assertThat(windows).hasSize(2);
    }

    @Test
    void enrichesEventsWithWatermarkContext() {
        WatermarkCoordinator watermarkCoord = new WatermarkCoordinator() {
            @Override
            public WatermarkWindow openWindow(TableId tableId, int chunkIndex) {
                return new WatermarkWindow(UUID.randomUUID(), tableId, Instant.now(), null, chunkIndex);
            }
            @Override
            public WatermarkWindow closeWindow(WatermarkWindow window) {
                return window.close(Instant.now());
            }
            @Override
            public void cancelWindow(WatermarkWindow window) { /* no-op in test */ }
        };

        DefaultSnapshotCoordinator.ChunkReader reader = plan -> {
            var event = new ChangeEnvelope(
                    UUID.randomUUID(), "test", TABLE,
                    OperationType.SNAPSHOT_READ, CaptureMode.SNAPSHOT,
                    new RowKey(Map.of("id", 1)),
                    null, Map.of("id", 1),
                    null, Instant.now(), null, Map.of()
            );
            return new DefaultSnapshotCoordinator.ChunkReader.ChunkReadResult(
                    List.of(event),
                    new SnapshotChunkResult(0, 1, new RowKey(Map.of("id", 1)), Duration.ofMillis(5))
            );
        };

        var receivedEvents = new ArrayList<ChangeEnvelope>();
        SnapshotCoordinator.ChunkHandler handler = (events, result) -> receivedEvents.addAll(events);

        var coordinator = new DefaultSnapshotCoordinator(
                watermarkCoord, new DefaultChunkPlanner(), reader, handler);
        coordinator.triggerSnapshot(TABLE, new SnapshotOptions(TABLE, 10));

        assertThat(receivedEvents).hasSize(1);
        assertThat(receivedEvents.getFirst().watermark()).isNotNull();
        assertThat(receivedEvents.getFirst().watermark().isClosed()).isTrue();
    }

    @Test
    void emptyTableProducesZeroChunks() {
        WatermarkCoordinator watermarkCoord = new WatermarkCoordinator() {
            @Override
            public WatermarkWindow openWindow(TableId tableId, int chunkIndex) {
                return new WatermarkWindow(UUID.randomUUID(), tableId, Instant.now(), null, chunkIndex);
            }
            @Override
            public WatermarkWindow closeWindow(WatermarkWindow window) {
                return window.close(Instant.now());
            }
            @Override
            public void cancelWindow(WatermarkWindow window) { /* no-op in test */ }
        };

        // Empty table: first chunk returns 0 rows
        DefaultSnapshotCoordinator.ChunkReader reader = plan ->
                new DefaultSnapshotCoordinator.ChunkReader.ChunkReadResult(
                        List.of(),
                        new SnapshotChunkResult(0, 0, null, Duration.ofMillis(1))
                );

        var chunks = new ArrayList<List<ChangeEnvelope>>();
        var coordinator = new DefaultSnapshotCoordinator(
                watermarkCoord, new DefaultChunkPlanner(), reader, (events, result) -> chunks.add(events));

        long total = coordinator.triggerSnapshot(TABLE, new SnapshotOptions(TABLE, 100));

        assertThat(total).isEqualTo(0);
        assertThat(chunks).hasSize(1);  // one empty chunk
        assertThat(chunks.getFirst()).isEmpty();
    }
}
