package com.paulsnow.qcdcf.postgres.watermark;

import com.paulsnow.qcdcf.core.exception.SourceReadException;
import com.paulsnow.qcdcf.core.watermark.WatermarkCoordinator;
import com.paulsnow.qcdcf.core.watermark.WatermarkWindow;
import com.paulsnow.qcdcf.model.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

/**
 * Writes watermark boundaries to the PostgreSQL watermark table and
 * implements the {@link WatermarkCoordinator} interface.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PostgresWatermarkWriter implements WatermarkCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresWatermarkWriter.class);

    private static final String INSERT_SQL = """
            INSERT INTO qcdcf_watermark (window_id, boundary, table_schema, table_name, chunk_index, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private final Connection connection;

    public PostgresWatermarkWriter(Connection connection) {
        this.connection = connection;
    }

    @Override
    public WatermarkWindow openWindow(TableId tableId, int chunkIndex) {
        UUID windowId = UUID.randomUUID();
        Instant now = Instant.now();
        writeBoundary(windowId, WatermarkBoundary.Boundary.LOW, tableId, chunkIndex, now);
        LOG.info("Opened watermark window {} for {} chunk {}", windowId, tableId, chunkIndex);
        return new WatermarkWindow(windowId, tableId, now, null, chunkIndex);
    }

    @Override
    public WatermarkWindow closeWindow(WatermarkWindow window) {
        Instant now = Instant.now();
        writeBoundary(window.windowId(), WatermarkBoundary.Boundary.HIGH,
                window.tableId(), window.chunkIndex(), now);
        LOG.info("Closed watermark window {}", window.windowId());
        return window.close(now);
    }

    private void writeBoundary(UUID windowId, WatermarkBoundary.Boundary boundary,
                               TableId tableId, int chunkIndex, Instant timestamp) {
        try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
            ps.setObject(1, windowId);
            ps.setString(2, boundary.name());
            ps.setString(3, tableId.schema());
            ps.setString(4, tableId.table());
            ps.setInt(5, chunkIndex);
            ps.setTimestamp(6, Timestamp.from(timestamp));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SourceReadException(
                    "Failed to write " + boundary + " watermark for window " + windowId, e);
        }
    }
}
