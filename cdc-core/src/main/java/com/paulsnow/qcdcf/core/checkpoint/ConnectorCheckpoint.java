package com.paulsnow.qcdcf.core.checkpoint;

import com.paulsnow.qcdcf.model.SourcePosition;

import java.time.Instant;

/**
 * Represents a durable checkpoint of connector progress.
 *
 * @param connectorId       the connector this checkpoint belongs to
 * @param position          the last confirmed WAL position
 * @param snapshotTableName the table currently being snapshotted (null if not snapshotting)
 * @param snapshotChunkIndex the last completed snapshot chunk index (-1 if not snapshotting)
 * @param timestamp         when this checkpoint was created
 * @author Paul Snow
 * @since 0.0.0
 */
public record ConnectorCheckpoint(
        String connectorId,
        SourcePosition position,
        String snapshotTableName,
        int snapshotChunkIndex,
        Instant timestamp
) {}
