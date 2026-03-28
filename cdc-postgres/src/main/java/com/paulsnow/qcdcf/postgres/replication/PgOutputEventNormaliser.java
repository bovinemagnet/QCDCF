package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Converts {@link DecodedReplicationMessage} instances into canonical {@link ChangeEnvelope} events.
 * <p>
 * Only data messages (INSERT, UPDATE, DELETE) are normalised. BEGIN and COMMIT messages
 * are filtered out by the caller ({@link PostgresLogStreamReader}).
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PgOutputEventNormaliser {

    private static final Logger LOG = LoggerFactory.getLogger(PgOutputEventNormaliser.class);

    private final String connectorId;

    public PgOutputEventNormaliser(String connectorId) {
        this.connectorId = connectorId;
    }

    /**
     * Normalises a decoded replication message into a ChangeEnvelope.
     * <p>
     * Extracts the row key from the message's key columns, maps the operation type,
     * and populates before/after value maps.
     *
     * @param message the decoded replication message (must be INSERT, UPDATE, or DELETE)
     * @return the normalised change envelope
     * @throws IllegalArgumentException if the message type is not a data change
     */
    public ChangeEnvelope normalise(DecodedReplicationMessage message) {
        if (message.tableId() == null) {
            throw new IllegalArgumentException("Cannot normalise non-data message: " + message.type());
        }

        RowKey rowKey = extractRowKey(message);
        OperationType operation = mapOperation(message.operation());

        // Data-change messages (INSERT/UPDATE/DELETE) decoded from pgoutput carry a null
        // commitTimestamp — only the BEGIN message receives the commit timestamp from the WAL.
        // Fall back to the current instant so SourcePosition's non-null contract is honoured.
        Instant captureNow = Instant.now();
        Instant commitTs = message.commitTimestamp() != null ? message.commitTimestamp() : captureNow;

        return new ChangeEnvelope(
                UUID.randomUUID(),
                connectorId,
                message.tableId(),
                operation,
                CaptureMode.LOG,
                rowKey,
                message.oldValues(),
                message.newValues(),
                new SourcePosition(message.lsn(), message.txId(), commitTs),
                captureNow,
                null,  // watermark context — populated during reconciliation
                Map.of()
        );
    }

    private RowKey extractRowKey(DecodedReplicationMessage message) {
        List<String> keyColumns = message.keyColumns();
        if (keyColumns == null || keyColumns.isEmpty()) {
            LOG.warn("No key columns for {} on {} at LSN {} — using all available values",
                    message.operation(), message.tableId(), message.lsn());
            // Fallback: use all values as key (REPLICA IDENTITY FULL scenario)
            Map<String, Object> values = message.newValues() != null ? message.newValues() : message.oldValues();
            return new RowKey(values != null ? values : Map.of());
        }

        // Extract key column values from new or old values
        Map<String, Object> values = message.newValues() != null ? message.newValues() : message.oldValues();
        if (values == null) {
            LOG.warn("No values available for key extraction at LSN {}", message.lsn());
            return new RowKey(Map.of("_unknown", message.lsn()));
        }

        Map<String, Object> keyValues = new LinkedHashMap<>();
        for (String col : keyColumns) {
            Object val = values.get(col);
            if (val != null) {
                keyValues.put(col, val);
            }
        }

        if (keyValues.isEmpty()) {
            LOG.warn("Key columns {} not found in values at LSN {}", keyColumns, message.lsn());
            return new RowKey(Map.of("_unknown", message.lsn()));
        }

        return new RowKey(keyValues);
    }

    private OperationType mapOperation(OperationType sourceOp) {
        // Direct mapping for standard operations
        return sourceOp;
    }
}
