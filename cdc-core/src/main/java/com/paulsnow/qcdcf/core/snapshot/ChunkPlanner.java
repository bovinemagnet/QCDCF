package com.paulsnow.qcdcf.core.snapshot;

import com.paulsnow.qcdcf.model.RowKey;

/**
 * Plans the next snapshot chunk based on the current position in the table.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public interface ChunkPlanner {

    /**
     * Plans the next chunk to read.
     *
     * @param options  the snapshot options
     * @param chunkIndex the zero-based index of the chunk
     * @param lastKey  the last key from the previous chunk (null for the first chunk)
     * @return the planned chunk, or null if no more chunks remain
     */
    SnapshotChunkPlan planNextChunk(SnapshotOptions options, int chunkIndex, RowKey lastKey);
}
