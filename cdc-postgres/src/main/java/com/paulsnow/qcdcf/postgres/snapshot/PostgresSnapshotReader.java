package com.paulsnow.qcdcf.postgres.snapshot;

import com.paulsnow.qcdcf.core.exception.SourceReadException;
import com.paulsnow.qcdcf.core.snapshot.SnapshotChunkPlan;
import com.paulsnow.qcdcf.core.snapshot.SnapshotChunkResult;
import com.paulsnow.qcdcf.model.*;
import com.paulsnow.qcdcf.postgres.metadata.TableMetadata;
import com.paulsnow.qcdcf.postgres.sql.PostgresChunkSqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Reads snapshot chunks from PostgreSQL using keyset pagination under READ COMMITTED isolation.
 * <p>
 * Each chunk is a bounded SELECT ordered by the primary key, using the last key from the
 * previous chunk as the exclusive lower bound. This avoids OFFSET-based pagination and
 * ensures consistent reads without long-running transactions or table locks.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PostgresSnapshotReader {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSnapshotReader.class);

    private final PostgresChunkSqlBuilder sqlBuilder;
    private final String connectorId;

    public PostgresSnapshotReader(PostgresChunkSqlBuilder sqlBuilder, String connectorId) {
        this.sqlBuilder = sqlBuilder;
        this.connectorId = connectorId;
    }

    /**
     * Reads a single snapshot chunk from the database.
     *
     * @param connection the database connection (should use READ COMMITTED isolation)
     * @param plan       the chunk plan describing what to read
     * @param metadata   the table metadata for PK column identification
     * @return the chunk result with the rows read as ChangeEnvelopes
     */
    public SnapshotChunkReadResult readChunk(Connection connection, SnapshotChunkPlan plan, TableMetadata metadata) {
        String sql = sqlBuilder.buildChunkQuery(plan, metadata.primaryKeyColumns());
        LOG.debug("Reading snapshot chunk {} for {}: {}", plan.chunkIndex(), plan.tableId(), sql);

        Instant start = Instant.now();
        List<ChangeEnvelope> events = new ArrayList<>();
        RowKey lastKey = null;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // Set keyset pagination parameters if there's a lower bound
            if (plan.lowerBound() != null) {
                int paramIndex = 1;
                for (String pkCol : metadata.primaryKeyColumns()) {
                    Object value = plan.lowerBound().columns().get(pkCol);
                    ps.setObject(paramIndex++, value);
                }
            }

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData rsMeta = rs.getMetaData();
                int columnCount = rsMeta.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>(columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(rsMeta.getColumnName(i), rs.getObject(i));
                    }

                    // Extract PK values for the row key
                    Map<String, Object> keyValues = new LinkedHashMap<>();
                    for (String pkCol : metadata.primaryKeyColumns()) {
                        keyValues.put(pkCol, row.get(pkCol));
                    }
                    lastKey = new RowKey(keyValues);

                    ChangeEnvelope envelope = new ChangeEnvelope(
                            UUID.randomUUID(),
                            connectorId,
                            plan.tableId(),
                            OperationType.SNAPSHOT_READ,
                            CaptureMode.SNAPSHOT,
                            lastKey,
                            null,  // no before values for snapshot reads
                            row,
                            null,  // no WAL position for snapshot reads
                            Instant.now(),
                            null,  // watermark context set by coordinator
                            Map.of()
                    );
                    events.add(envelope);
                }
            }
        } catch (SQLException e) {
            throw new SourceReadException(
                    "Failed to read snapshot chunk " + plan.chunkIndex() + " for " + plan.tableId(), e);
        }

        Duration duration = Duration.between(start, Instant.now());
        SnapshotChunkResult result = new SnapshotChunkResult(
                plan.chunkIndex(), events.size(), lastKey, duration);

        LOG.info("Snapshot chunk {} for {}: {} rows in {}ms",
                plan.chunkIndex(), plan.tableId(), events.size(), duration.toMillis());

        return new SnapshotChunkReadResult(result, events);
    }

    /**
     * Holds both the chunk metadata result and the actual row events.
     */
    public record SnapshotChunkReadResult(
            SnapshotChunkResult result,
            List<ChangeEnvelope> events
    ) {}
}
