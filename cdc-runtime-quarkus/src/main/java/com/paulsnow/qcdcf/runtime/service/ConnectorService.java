package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.connector.ConnectorStatus;
import com.paulsnow.qcdcf.runtime.bootstrap.ConnectorBootstrap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

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

    @Inject
    ConnectorBootstrap bootstrap;

    private final Instant startTime = Instant.now();
    private volatile String lastSnapshotTable;
    private volatile String lastSnapshotStatus = "NONE";

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
     * Trigger a snapshot for the specified table.
     * <p>
     * This is currently a stub that acknowledges the request.
     * Actual snapshot execution will be implemented in a future phase.
     *
     * @param tableName the fully-qualified table name to snapshot
     * @return status map acknowledging the snapshot request
     */
    public Map<String, Object> triggerSnapshot(String tableName) {
        LOG.infof("Snapshot requested for table '%s' on connector '%s'", tableName, bootstrap.connectorId());
        lastSnapshotTable = tableName;
        lastSnapshotStatus = "REQUESTED";
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connectorId", bootstrap.connectorId());
        result.put("tableName", tableName);
        result.put("snapshotStatus", lastSnapshotStatus);
        result.put("message", "Snapshot request acknowledged — execution deferred");
        return result;
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
        result.put("lastSnapshotTable", lastSnapshotTable != null ? lastSnapshotTable : "N/A");
        result.put("lastSnapshotStatus", lastSnapshotStatus);
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
        return lastSnapshotTable;
    }

    /**
     * Return the last snapshot status string.
     */
    public String lastSnapshotStatus() {
        return lastSnapshotStatus;
    }

    private static String formatDuration(Duration duration) {
        long hours = duration.toHours();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }
}
