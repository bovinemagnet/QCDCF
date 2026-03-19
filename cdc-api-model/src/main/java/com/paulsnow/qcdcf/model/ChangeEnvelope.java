package com.paulsnow.qcdcf.model;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * The canonical event envelope emitted by the CDC pipeline.
 * <p>
 * Every change — whether from the WAL, a snapshot read, or reconciliation —
 * is wrapped in this envelope before delivery to a sink.
 *
 * @param eventId      unique identifier for this event
 * @param connectorId  the connector that produced this event
 * @param tableId      the source table
 * @param operation    the type of change
 * @param captureMode  how this event was captured
 * @param key          the primary key of the affected row
 * @param before       column values before the change (null for INSERT and SNAPSHOT_READ)
 * @param after        column values after the change (null for DELETE)
 * @param position     the WAL position (null for snapshot-only events)
 * @param captureTimestamp  when this event was captured by the framework
 * @param watermark    watermark context if this event is part of a snapshot window (may be null)
 * @param metadata     extensible metadata for tracing headers, schema version, etc.
 * @author Paul Snow
 * @since 0.0.0
 */
public record ChangeEnvelope(
        UUID eventId,
        String connectorId,
        TableId tableId,
        OperationType operation,
        CaptureMode captureMode,
        RowKey key,
        Map<String, Object> before,
        Map<String, Object> after,
        SourcePosition position,
        Instant captureTimestamp,
        WatermarkContext watermark,
        Map<String, String> metadata
) {

    public ChangeEnvelope {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(connectorId, "connectorId must not be null");
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(captureMode, "captureMode must not be null");
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(captureTimestamp, "captureTimestamp must not be null");
        // Use Collections.unmodifiableMap instead of Map.copyOf because
        // column values may be null (database NULL), and Map.copyOf rejects nulls.
        before = before != null ? java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(before)) : null;
        after = after != null ? java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(after)) : null;
        metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
    }
}
