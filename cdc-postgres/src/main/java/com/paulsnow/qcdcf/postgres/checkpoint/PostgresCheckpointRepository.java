package com.paulsnow.qcdcf.postgres.checkpoint;

import com.paulsnow.qcdcf.core.checkpoint.CheckpointManager;
import com.paulsnow.qcdcf.core.checkpoint.ConnectorCheckpoint;
import com.paulsnow.qcdcf.core.exception.CheckpointException;
import com.paulsnow.qcdcf.model.SourcePosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.Optional;

/**
 * PostgreSQL-backed implementation of {@link CheckpointManager}.
 * <p>
 * Persists connector checkpoints to the {@code qcdcf_checkpoint} table,
 * allowing the connector to resume from its last known position after restart.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PostgresCheckpointRepository implements CheckpointManager {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCheckpointRepository.class);

    private static final String UPSERT_SQL = """
            INSERT INTO qcdcf_checkpoint (connector_id, last_lsn, last_tx_id, snapshot_table, snapshot_chunk_index, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (connector_id)
            DO UPDATE SET last_lsn = EXCLUDED.last_lsn,
                          last_tx_id = EXCLUDED.last_tx_id,
                          snapshot_table = EXCLUDED.snapshot_table,
                          snapshot_chunk_index = EXCLUDED.snapshot_chunk_index,
                          updated_at = EXCLUDED.updated_at
            """;

    private static final String SELECT_SQL = """
            SELECT connector_id, last_lsn, last_tx_id, snapshot_table, snapshot_chunk_index, updated_at
            FROM qcdcf_checkpoint
            WHERE connector_id = ?
            """;

    private final Connection connection;

    public PostgresCheckpointRepository(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void save(ConnectorCheckpoint checkpoint) {
        try (PreparedStatement ps = connection.prepareStatement(UPSERT_SQL)) {
            ps.setString(1, checkpoint.connectorId());
            ps.setLong(2, checkpoint.position().lsn());
            if (checkpoint.position().txId() != null) {
                ps.setLong(3, checkpoint.position().txId());
            } else {
                ps.setNull(3, Types.BIGINT);
            }
            ps.setString(4, checkpoint.snapshotTableName());
            ps.setInt(5, checkpoint.snapshotChunkIndex());
            ps.setTimestamp(6, Timestamp.from(checkpoint.timestamp()));
            ps.executeUpdate();
            LOG.debug("Saved checkpoint for connector '{}': LSN={}", checkpoint.connectorId(), checkpoint.position().lsn());
        } catch (SQLException e) {
            throw new CheckpointException("Failed to save checkpoint for: " + checkpoint.connectorId(), e);
        }
    }

    @Override
    public Optional<ConnectorCheckpoint> load(String connectorId) {
        try (PreparedStatement ps = connection.prepareStatement(SELECT_SQL)) {
            ps.setString(1, connectorId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Long txId = rs.getObject("last_tx_id", Long.class);
                    Timestamp updatedAt = rs.getTimestamp("updated_at");
                    ConnectorCheckpoint checkpoint = new ConnectorCheckpoint(
                            rs.getString("connector_id"),
                            new SourcePosition(rs.getLong("last_lsn"), txId,
                                    updatedAt != null ? updatedAt.toInstant() : Instant.now()),
                            rs.getString("snapshot_table"),
                            rs.getInt("snapshot_chunk_index"),
                            updatedAt != null ? updatedAt.toInstant() : Instant.now()
                    );
                    LOG.debug("Loaded checkpoint for connector '{}': LSN={}", connectorId, checkpoint.position().lsn());
                    return Optional.of(checkpoint);
                }
                LOG.debug("No checkpoint found for connector '{}'", connectorId);
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new CheckpointException("Failed to load checkpoint for: " + connectorId, e);
        }
    }

    /** SQL DDL for creating the checkpoint table. */
    public static final String CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS qcdcf_checkpoint (
                connector_id       VARCHAR(255) PRIMARY KEY,
                last_lsn           BIGINT NOT NULL,
                last_tx_id         BIGINT,
                snapshot_table     VARCHAR(512),
                snapshot_chunk_index INTEGER NOT NULL DEFAULT -1,
                updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )
            """;
}
