package com.paulsnow.qcdcf.core.snapshot;

import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.model.TableId;

import java.util.List;

/**
 * Coordinates a full-table snapshot by orchestrating chunk reads with watermark windows.
 * <p>
 * For each chunk:
 * <ol>
 *   <li>Open a watermark window (write low watermark)</li>
 *   <li>Read the chunk using keyset pagination</li>
 *   <li>Close the watermark window (write high watermark)</li>
 *   <li>Hand the chunk data and window to the reconciliation engine</li>
 * </ol>
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public interface SnapshotCoordinator {

    /**
     * Triggers a full-table snapshot for the given table.
     *
     * @param tableId   the table to snapshot
     * @param options   the snapshot options (chunk size, etc.)
     * @return the total number of rows read across all chunks
     */
    long triggerSnapshot(TableId tableId, SnapshotOptions options);

    /**
     * Callback interface for receiving snapshot chunk data within a watermark window.
     */
    @FunctionalInterface
    interface ChunkHandler {
        /**
         * Called for each completed snapshot chunk.
         *
         * @param chunkEvents the snapshot row events for this chunk
         * @param chunkResult metadata about the chunk read
         */
        void onChunk(List<ChangeEnvelope> chunkEvents, SnapshotChunkResult chunkResult);
    }
}
