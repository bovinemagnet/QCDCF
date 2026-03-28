package com.paulsnow.qcdcf.core.snapshot;

import com.paulsnow.qcdcf.model.RowKey;

/**
 * Default chunk planner that creates sequential chunks using keyset pagination.
 * <p>
 * Each chunk uses the last key from the previous chunk as its exclusive lower bound.
 * The planner does not know when the table is exhausted — the caller should stop
 * when a chunk returns fewer rows than the chunk size.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class DefaultChunkPlanner implements ChunkPlanner {

    @Override
    public SnapshotChunkPlan planNextChunk(SnapshotOptions options, int chunkIndex, RowKey lastKey) {
        return new SnapshotChunkPlan(
                options.tableId(),
                chunkIndex,
                options.chunkSize(),
                lastKey,
                null  // upper bound determined at read time
        );
    }
}
