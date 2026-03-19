package com.paulsnow.qcdcf.core.checkpoint;

import com.paulsnow.qcdcf.model.RowKey;
import com.paulsnow.qcdcf.model.TableId;

import java.time.Instant;

/**
 * Tracks the progress of an active snapshot operation.
 *
 * @param tableId          the table being snapshotted
 * @param totalChunks      the number of chunks completed so far
 * @param totalRows        the total rows read so far
 * @param lastKey          the last row key read (resume point)
 * @param startedAt        when the snapshot was initiated
 * @param lastChunkAt      when the last chunk was completed
 * @param complete         whether the snapshot has finished
 * @author Paul Snow
 * @since 0.0.0
 */
public record SnapshotProgress(
        TableId tableId,
        int totalChunks,
        long totalRows,
        RowKey lastKey,
        Instant startedAt,
        Instant lastChunkAt,
        boolean complete
) {

    /** Creates initial progress for a new snapshot. */
    public static SnapshotProgress start(TableId tableId) {
        return new SnapshotProgress(tableId, 0, 0, null, Instant.now(), null, false);
    }

    /** Returns updated progress after completing a chunk. */
    public SnapshotProgress advanceChunk(int rowsInChunk, RowKey lastKey) {
        return new SnapshotProgress(
                tableId, totalChunks + 1, totalRows + rowsInChunk,
                lastKey, startedAt, Instant.now(), false
        );
    }

    /** Returns completed progress. */
    public SnapshotProgress markComplete() {
        return new SnapshotProgress(
                tableId, totalChunks, totalRows, lastKey,
                startedAt, Instant.now(), true
        );
    }
}
