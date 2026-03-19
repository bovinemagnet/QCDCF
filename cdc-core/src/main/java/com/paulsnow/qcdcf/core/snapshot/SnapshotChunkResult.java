package com.paulsnow.qcdcf.core.snapshot;

import com.paulsnow.qcdcf.model.RowKey;

import java.time.Duration;

/**
 * Result of executing a snapshot chunk read.
 *
 * @param chunkIndex  the index of the chunk that was read
 * @param rowCount    the number of rows returned
 * @param lastKey     the last key read (used as the lower bound for the next chunk)
 * @param duration    how long the chunk read took
 * @author Paul Snow
 * @since 0.0.0
 */
public record SnapshotChunkResult(
        int chunkIndex,
        int rowCount,
        RowKey lastKey,
        Duration duration
) {}
