package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.connector.ConnectorStatus;
import com.paulsnow.qcdcf.core.exception.SourceReadException;
import com.paulsnow.qcdcf.core.snapshot.DefaultChunkPlanner;
import com.paulsnow.qcdcf.core.snapshot.DefaultSnapshotCoordinator;
import com.paulsnow.qcdcf.core.snapshot.SnapshotOptions;
import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.postgres.metadata.PostgresTableMetadataReader;
import com.paulsnow.qcdcf.postgres.metadata.TableMetadata;
import com.paulsnow.qcdcf.postgres.snapshot.PostgresSnapshotReader;
import com.paulsnow.qcdcf.postgres.sql.PostgresChunkSqlBuilder;
import com.paulsnow.qcdcf.postgres.watermark.PostgresWatermarkWriter;
import com.paulsnow.qcdcf.runtime.bootstrap.ConnectorBootstrap;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Service layer wrapping connector operations.
 * <p>
 * Provides business logic for pausing, resuming, and triggering snapshots
 * on the CDC connector, as well as progress reporting.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class ConnectorService {

    private static final Logger LOG = Logger.getLogger(ConnectorService.class);

    private static final String STATUS_RUNNING = "RUNNING";

    public record SnapshotState(String table, String status, long rowCount) {
        static final SnapshotState NONE = new SnapshotState(null, "NONE", 0);
    }

    @Inject
    ConnectorBootstrap bootstrap;

    @Inject
    ConnectorRuntimeConfig config;

    @Inject
    EventSink eventSink;

    @Inject
    DataSource dataSource;

    @Inject
    SnapshotMonitorService snapshotMonitor;

    @Inject
    ManagedExecutor executor;

    private final Instant startTime = Instant.now();
    private final AtomicReference<SnapshotState> snapshotState =
            new AtomicReference<>(SnapshotState.NONE);

    /**
     * Pause the connector by stopping the WAL reader.
     *
     * @return status map confirming the pause
     */
    public Map<String, Object> pause() {
        LOG.infof("Pause requested for connector '%s'", bootstrap.connectorId());
        bootstrap.requestStop();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connectorId", bootstrap.connectorId());
        result.put("status", bootstrap.status().name());
        result.put("message", "Connector paused");
        return result;
    }

    /**
     * Resume the connector by restarting the WAL reader.
     *
     * @return status map confirming the resume
     */
    public Map<String, Object> resume() {
        LOG.infof("Resume requested for connector '%s'", bootstrap.connectorId());
        bootstrap.requestStart();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connectorId", bootstrap.connectorId());
        result.put("status", bootstrap.status().name());
        result.put("message", "Connector resumed");
        return result;
    }

    /**
     * Triggers a snapshot for the specified table.
     * <p>
     * Parses the table name as {@code schema.table} (defaults to {@code public} schema),
     * loads metadata, and runs the snapshot coordinator on a background thread.
     *
     * @param tableName the fully-qualified table name (e.g. "public.customer")
     * @return status map acknowledging the snapshot request
     */
    public Map<String, Object> triggerSnapshot(String tableName) {
        LOG.infof("Snapshot requested for table '%s' on connector '%s'", tableName, bootstrap.connectorId());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connectorId", bootstrap.connectorId());
        result.put("tableName", tableName);

        if (!tryStartSnapshot(tableName)) {
            LOG.warnf("Snapshot request for '%s' rejected — a snapshot is already running for '%s'",
                    tableName, snapshotState.get().table());
            result.put("snapshotStatus", snapshotState.get().status());
            result.put("message", "Snapshot already running; request ignored");
            return result;
        }

        // Parse table name
        String schema = "public";
        String table = tableName;
        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.", 2);
            schema = parts[0];
            table = parts[1];
        }
        TableId tableId = new TableId(schema, table);

        // Run snapshot on background thread to avoid blocking the REST call
        executor.submit(() -> {
            executeSnapshot(tableId);
        });

        result.put("snapshotStatus", snapshotState.get().status());
        result.put("message", "Snapshot started");
        return result;
    }

    /**
     * Runs a snapshot for the given table synchronously, blocking until it
     * completes or fails. Used by the scheduler to run tables sequentially.
     *
     * @param tableId the table to snapshot
     * @return {@code true} if the snapshot was started and completed successfully,
     *         {@code false} if it failed or another snapshot is already running
     */
    public boolean runSnapshotBlocking(TableId tableId) {
        if (!tryStartSnapshot(tableId.canonicalName())) {
            LOG.warnf("Blocking snapshot for '%s' rejected — a snapshot is already running for '%s'",
                    tableId, snapshotState.get().table());
            return false;
        }
        return executeSnapshot(tableId);
    }

    private boolean executeSnapshot(TableId tableId) {
        snapshotMonitor.recordSnapshotStarted(tableId.canonicalName());
        try {
            long rows = executeSnapshotWithRetry(tableId);
            snapshotState.set(new SnapshotState(tableId.canonicalName(), "COMPLETE (" + rows + " rows)", rows));
            snapshotMonitor.recordSnapshotCompleted(tableId.canonicalName(), rows);
            LOG.infof("Snapshot complete for %s: %d rows", tableId, rows);
            return true;
        } catch (Exception e) {
            snapshotState.set(new SnapshotState(tableId.canonicalName(), "FAILED: " + e.getMessage(), 0));
            snapshotMonitor.recordSnapshotFailed(tableId.canonicalName(), e.getMessage());
            LOG.errorf(e, "Snapshot failed for %s after retries", tableId);
            return false;
        }
    }

    public boolean isSnapshotRunning() {
        return STATUS_RUNNING.equals(snapshotState.get().status());
    }

    /**
     * Atomically transitions the snapshot state to RUNNING for the given table,
     * unless a snapshot is already in progress.
     *
     * @return {@code true} if the state was claimed, {@code false} if busy
     */
    private boolean tryStartSnapshot(String tableName) {
        while (true) {
            SnapshotState current = snapshotState.get();
            if (STATUS_RUNNING.equals(current.status())) {
                return false;
            }
            if (snapshotState.compareAndSet(current, new SnapshotState(tableName, STATUS_RUNNING, 0))) {
                return true;
            }
        }
    }

    @Retry(maxRetries = 3, delay = 2000, retryOn = {SourceReadException.class, SQLException.class})
    long executeSnapshotWithRetry(TableId tableId) throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            // Load metadata
            var metadataReader = new PostgresTableMetadataReader();
            TableMetadata metadata = metadataReader.loadTableMetadata(conn, tableId);

            // Build snapshot pipeline
            var sqlBuilder = new PostgresChunkSqlBuilder();
            var snapshotReader = new PostgresSnapshotReader(sqlBuilder, bootstrap.connectorId());
            var watermarkCoord = new PostgresWatermarkWriter(conn);

            var coordinator = new DefaultSnapshotCoordinator(
                    watermarkCoord,
                    new DefaultChunkPlanner(),
                    (DefaultSnapshotCoordinator.ChunkReader) plan -> {
                        PostgresSnapshotReader.SnapshotChunkReadResult readResult =
                                snapshotReader.readChunk(conn, plan, metadata);
                        return new DefaultSnapshotCoordinator.ChunkReader.ChunkReadResult(
                                readResult.events(), readResult.result());
                    },
                    (events, chunkResult) -> events.forEach(eventSink::publish)
            );

            int chunkSize = config.source().chunkSize();
            return coordinator.triggerSnapshot(tableId, new SnapshotOptions(tableId, chunkSize));
        }
    }

    /**
     * Return current progress information for the connector.
     *
     * @return map containing status, uptime, and snapshot information
     */
    public Map<String, Object> getProgress() {
        Duration uptime = Duration.between(startTime, Instant.now());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connectorId", bootstrap.connectorId());
        result.put("status", bootstrap.status().name());
        result.put("uptimeSeconds", uptime.toSeconds());
        result.put("uptime", formatDuration(uptime));
        result.put("startTime", startTime.toString());
        SnapshotState snapshot = snapshotState.get();
        result.put("lastSnapshotTable", snapshot.table() != null ? snapshot.table() : "N/A");
        result.put("lastSnapshotStatus", snapshot.status());
        return result;
    }

    /**
     * Return the current connector status.
     */
    public ConnectorStatus status() {
        return bootstrap.status();
    }

    /**
     * Return the connector identifier.
     */
    public String connectorId() {
        return bootstrap.connectorId();
    }

    /**
     * Return the instant the service was initialised.
     */
    public Instant startTime() {
        return startTime;
    }

    /**
     * Return the last snapshot table name, or {@code null} if none requested.
     */
    public String lastSnapshotTable() {
        return snapshotState.get().table();
    }

    /**
     * Return the last snapshot status string.
     */
    public String lastSnapshotStatus() {
        return snapshotState.get().status();
    }

    /**
     * Return the instant of the last successful WAL connection, or {@code null} if never connected.
     */
    public Instant lastSuccessfulConnection() { return bootstrap.lastSuccessfulConnection(); }

    /**
     * Return {@code true} if the checkpoint circuit breaker is currently open.
     */
    public boolean isCheckpointCircuitOpen() { return bootstrap.isCheckpointCircuitOpen(); }

    /**
     * Return the number of consecutive checkpoint save failures.
     */
    public int checkpointConsecutiveFailures() { return bootstrap.checkpointConsecutiveFailures(); }

    private static String formatDuration(Duration duration) {
        long hours = duration.toHours();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }
}
