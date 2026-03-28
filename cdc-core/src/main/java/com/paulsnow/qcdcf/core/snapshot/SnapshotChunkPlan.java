package com.paulsnow.qcdcf.core.snapshot;

import com.paulsnow.qcdcf.model.RowKey;
import com.paulsnow.qcdcf.model.TableId;

/**
 * Describes a planned snapshot chunk with key boundaries.
 *
 * @param tableId     the table being snapshotted
 * @param chunkIndex  the zero-based index of this chunk
 * @param chunkSize   the maximum number of rows to read
 * @param lowerBound  the exclusive lower bound key (null for the first chunk)
 * @param upperBound  the inclusive upper bound key (null for the last chunk)
 * @author Paul Snow
 * @since 0.0.0
 */
public record SnapshotChunkPlan(
        TableId tableId,
        int chunkIndex,
        int chunkSize,
        RowKey lowerBound,
        RowKey upperBound
) {}
