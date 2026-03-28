package com.paulsnow.qcdcf.core.snapshot;

import com.paulsnow.qcdcf.core.watermark.WatermarkCoordinator;
import com.paulsnow.qcdcf.core.watermark.WatermarkWindow;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.model.RowKey;
import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.model.WatermarkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Default implementation of {@link SnapshotCoordinator} that reads chunks
 * in a loop, bracketing each with watermark writes.
 * <p>
 * Uses a {@link ChunkReader} functional interface to abstract the actual
 * database read, keeping this coordinator in the core module (no JDBC dependency).
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class DefaultSnapshotCoordinator implements SnapshotCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSnapshotCoordinator.class);

    private final WatermarkCoordinator watermarkCoordinator;
    private final ChunkPlanner chunkPlanner;
    private final ChunkReader chunkReader;
    private final ChunkHandler chunkHandler;

    /**
     * Functional interface for reading a snapshot chunk.
     * Implemented by the PostgreSQL module's snapshot reader.
     */
    @FunctionalInterface
    public interface ChunkReader {
        /**
         * Reads a single snapshot chunk and returns the events.
         *
         * @param plan the chunk plan
         * @return the chunk events and metadata
         */
        ChunkReadResult read(SnapshotChunkPlan plan);

        record ChunkReadResult(List<ChangeEnvelope> events, SnapshotChunkResult result) {}
    }

    public DefaultSnapshotCoordinator(WatermarkCoordinator watermarkCoordinator,
                                      ChunkPlanner chunkPlanner,
                                      ChunkReader chunkReader,
                                      ChunkHandler chunkHandler) {
        this.watermarkCoordinator = watermarkCoordinator;
        this.chunkPlanner = chunkPlanner;
        this.chunkReader = chunkReader;
        this.chunkHandler = chunkHandler;
    }

    @Override
    public long triggerSnapshot(TableId tableId, SnapshotOptions options) {
        LOG.info("Starting snapshot for table {} with chunk size {}", tableId, options.chunkSize());

        long totalRows = 0;
        int chunkIndex = 0;
        RowKey lastKey = null;

        while (true) {
            SnapshotChunkPlan plan = chunkPlanner.planNextChunk(options, chunkIndex, lastKey);
            if (plan == null) {
                break;  // no more chunks
            }

            // 1. Open watermark window
            WatermarkWindow window = watermarkCoordinator.openWindow(tableId, chunkIndex);

            List<ChangeEnvelope> enrichedEvents;
            ChunkReader.ChunkReadResult readResult;
            try {
                // 2. Read the chunk
                readResult = chunkReader.read(plan);
                List<ChangeEnvelope> chunkEvents = readResult.events();

                // 3. Close watermark window
                WatermarkWindow closedWindow = watermarkCoordinator.closeWindow(window);

                // 4. Enrich events with watermark context
                WatermarkContext ctx = new WatermarkContext(
                        closedWindow.windowId(), closedWindow.lowMark(), closedWindow.highMark());
                enrichedEvents = chunkEvents.stream()
                        .map(e -> new ChangeEnvelope(
                                e.eventId(), e.connectorId(), e.tableId(), e.operation(),
                                e.captureMode(), e.key(), e.before(), e.after(),
                                e.position(), e.captureTimestamp(), ctx, e.metadata()))
                        .toList();
            } catch (Exception e) {
                watermarkCoordinator.cancelWindow(window);
                throw e;
            }

            // 5. Hand to the chunk handler (reconciliation pipeline)
            chunkHandler.onChunk(enrichedEvents, readResult.result());

            totalRows += readResult.result().rowCount();
            lastKey = readResult.result().lastKey();
            chunkIndex++;

            LOG.info("Snapshot chunk {} for {}: {} rows (total: {})",
                    chunkIndex - 1, tableId, readResult.result().rowCount(), totalRows);

            // If the chunk returned fewer rows than the chunk size, we're done
            if (readResult.result().rowCount() < options.chunkSize()) {
                break;
            }
        }

        LOG.info("Snapshot complete for {}: {} chunks, {} total rows", tableId, chunkIndex, totalRows);
        return totalRows;
    }
}
