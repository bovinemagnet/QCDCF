package com.paulsnow.qcdcf.core.snapshot;

import com.paulsnow.qcdcf.model.TableId;

import java.util.Objects;

/**
 * Configuration options for a snapshot operation.
 *
 * @param tableId   the table to snapshot
 * @param chunkSize number of rows per chunk
 * @author Paul Snow
 * @since 0.0.0
 */
public record SnapshotOptions(TableId tableId, int chunkSize) {

    public static final int DEFAULT_CHUNK_SIZE = 1000;

    public SnapshotOptions {
        Objects.requireNonNull(tableId, "tableId must not be null");
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive, got: " + chunkSize);
        }
    }

    public SnapshotOptions(TableId tableId) {
        this(tableId, DEFAULT_CHUNK_SIZE);
    }
}
