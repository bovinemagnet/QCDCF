package com.paulsnow.qcdcf.runtime.dashboard;

import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.postgres.metadata.PostgresTableMetadataReader;
import com.paulsnow.qcdcf.postgres.metadata.TableMetadata;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import com.paulsnow.qcdcf.runtime.service.ConnectorService;
import com.paulsnow.qcdcf.runtime.service.MetricsService;
import com.paulsnow.qcdcf.runtime.service.ReplicationHealthService;
import com.paulsnow.qcdcf.runtime.service.SnapshotMonitorService;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serves the HTMX-based operational dashboard.
 * <p>
 * All views are server-rendered HTML fragments suitable for HTMX consumption.
 * Minimal JavaScript — live updates via HTMX polling. Sparkline charts are
 * rendered server-side as inline SVG.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Path("/dashboard")
public class DashboardResource {

    /* ── Colour constants for sparklines ─────────────────────────────── */
    private static final String COLOUR_INDIGO = "#818cf8";
    private static final String COLOUR_EMERALD = "#34d399";
    private static final String COLOUR_RED = "#f87171";
    private static final String COLOUR_BLUE = "#60a5fa";
    private static final String COLOUR_PURPLE = "#a78bfa";

    /* ── Small sparkline dimensions (metric cards) ───────────────────── */
    private static final int SPARK_SM_W = 120;
    private static final int SPARK_SM_H = 30;

    /* ── Large sparkline dimensions (history page) ───────────────────── */
    private static final int SPARK_LG_W = 800;
    private static final int SPARK_LG_H = 80;

    @Inject
    Template dashboard;

    @Inject
    Template history;

    @Inject
    Template statusFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/metricsCards")
    Template metricsCardsFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/recentEvents")
    Template recentEventsFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/eventBreakdown")
    Template eventBreakdownFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/errors")
    Template errorsFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/historyTable")
    Template historyTableFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/monitoredTables")
    Template monitoredTablesFragment;

    @Inject
    Template snapshots;

    @Inject
    @io.quarkus.qute.Location("fragments/snapshotActive")
    Template snapshotActiveFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/watermarkWindows")
    Template watermarkWindowsFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/watermarkWalEvents")
    Template watermarkWalEventsFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/snapshotHistory")
    Template snapshotHistoryFragment;

    @Inject
    Template replication;

    @Inject
    @io.quarkus.qute.Location("fragments/replicationHealth")
    Template replicationHealthFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/replicationSlotDetail")
    Template replicationSlotDetailFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/replicationConfig")
    Template replicationConfigFragment;

    @Inject
    @io.quarkus.qute.Location("fragments/replicationDanger")
    Template replicationDangerFragment;

    @Inject
    ConnectorService connectorService;

    @Inject
    MetricsService metricsService;

    @Inject
    SnapshotMonitorService snapshotMonitorService;

    @Inject
    ReplicationHealthService replicationHealthService;

    @Inject
    ConnectorRuntimeConfig config;

    @Inject
    DataSource dataSource;

    // ── Main pages ──────────────────────────────────────────────────────

    /**
     * Main dashboard page.
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance index() {
        return buildFullDashboardData(dashboard);
    }

    /**
     * Historical metrics page with large sparkline charts and snapshot table.
     */
    @GET
    @Path("/history")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance historyPage() {
        return history
                .data("eventsLargeSparkline", SparklineGenerator.generate(
                        metricsService.eventsPerSecondHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_EMERALD))
                .data("insertsLargeSparkline", SparklineGenerator.generate(
                        metricsService.insertRateHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_INDIGO))
                .data("updatesLargeSparkline", SparklineGenerator.generate(
                        metricsService.updateRateHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_BLUE))
                .data("deletesLargeSparkline", SparklineGenerator.generate(
                        metricsService.deleteRateHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_PURPLE))
                .data("errorsLargeSparkline", SparklineGenerator.generate(
                        metricsService.errorRateHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_RED))
                .data("snapshots", metricsService.historySnapshots());
    }

    /**
     * Snapshots and watermarks monitoring page.
     */
    @GET
    @Path("/snapshots")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance snapshotsPage() {
        return snapshots
                .data("activeSnapshot", snapshotMonitorService.activeSnapshot())
                .data("activeWindows", snapshotMonitorService.activeWindows())
                .data("completedWindows", snapshotMonitorService.completedWindows())
                .data("recentWatermarkEvents", snapshotMonitorService.recentWatermarkEvents())
                .data("totalWatermarkEvents", snapshotMonitorService.totalWatermarkEventsDetected())
                .data("snapshotHistory", snapshotMonitorService.snapshotHistory());
    }

    // ── Fragment endpoints (HTMX polling) ───────────────────────────────

    /**
     * Status fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/status")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance statusFragment() {
        return buildStatusData(statusFragment);
    }

    /**
     * Metrics cards fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/metrics")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance metricsFragment() {
        return buildMetricsData(metricsCardsFragment);
    }

    /**
     * Recent events fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/recent-events")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance recentEventsFragment() {
        return recentEventsFragment
                .data("recentEvents", metricsService.recentEvents());
    }

    /**
     * Event breakdown fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/breakdown")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance breakdownFragment() {
        return buildBreakdownData(eventBreakdownFragment);
    }

    /**
     * Error log fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/errors")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance errorsFragment() {
        return errorsFragment
                .data("recentErrors", metricsService.recentErrors());
    }

    /**
     * Monitored tables fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/tables")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance tablesFragment() {
        return buildTablesData(monitoredTablesFragment);
    }

    // ── History sparkline fragments (HTMX polling) ──────────────────────

    /**
     * Large events-per-second sparkline fragment.
     */
    @GET
    @Path("/fragments/history-events")
    @Produces(MediaType.TEXT_HTML)
    public String historyEventsFragment() {
        return SparklineGenerator.generate(
                metricsService.eventsPerSecondHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_EMERALD);
    }

    /**
     * Large inserts-per-second sparkline fragment.
     */
    @GET
    @Path("/fragments/history-inserts")
    @Produces(MediaType.TEXT_HTML)
    public String historyInsertsFragment() {
        return SparklineGenerator.generate(
                metricsService.insertRateHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_INDIGO);
    }

    /**
     * Large updates-per-second sparkline fragment.
     */
    @GET
    @Path("/fragments/history-updates")
    @Produces(MediaType.TEXT_HTML)
    public String historyUpdatesFragment() {
        return SparklineGenerator.generate(
                metricsService.updateRateHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_BLUE);
    }

    /**
     * Large deletes-per-second sparkline fragment.
     */
    @GET
    @Path("/fragments/history-deletes")
    @Produces(MediaType.TEXT_HTML)
    public String historyDeletesFragment() {
        return SparklineGenerator.generate(
                metricsService.deleteRateHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_PURPLE);
    }

    /**
     * Large errors-per-second sparkline fragment.
     */
    @GET
    @Path("/fragments/history-errors")
    @Produces(MediaType.TEXT_HTML)
    public String historyErrorsFragment() {
        return SparklineGenerator.generate(
                metricsService.errorRateHistory(), SPARK_LG_W, SPARK_LG_H, COLOUR_RED);
    }

    /**
     * History table fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/history-table")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance historyTableFragment() {
        return historyTableFragment
                .data("snapshots", metricsService.historySnapshots());
    }

    // ── Snapshot & watermark fragments (HTMX polling) ─────────────────

    /**
     * Active snapshot status fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/snapshot-active")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance snapshotActiveFragment() {
        return snapshotActiveFragment
                .data("activeSnapshot", snapshotMonitorService.activeSnapshot());
    }

    /**
     * Watermark windows fragment (active + completed) for HTMX polling.
     */
    @GET
    @Path("/fragments/watermark-windows")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance watermarkWindowsFragment() {
        return watermarkWindowsFragment
                .data("activeWindows", snapshotMonitorService.activeWindows())
                .data("completedWindows", snapshotMonitorService.completedWindows());
    }

    /**
     * Watermark WAL events fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/watermark-wal")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance watermarkWalFragment() {
        return watermarkWalEventsFragment
                .data("recentWatermarkEvents", snapshotMonitorService.recentWatermarkEvents())
                .data("totalWatermarkEvents", snapshotMonitorService.totalWatermarkEventsDetected());
    }

    /**
     * Snapshot job history fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/snapshot-history")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance snapshotHistoryFragment() {
        return snapshotHistoryFragment
                .data("snapshotHistory", snapshotMonitorService.snapshotHistory());
    }

    // ── Replication health page + fragments ─────────────────────────────

    /**
     * Replication health monitoring page.
     */
    @GET
    @Path("/replication")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance replicationPage() {
        return buildReplicationData(replication);
    }

    /**
     * Replication health cards fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/replication-health")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance replicationHealthFragment() {
        return buildReplicationHealthData(replicationHealthFragment);
    }

    /**
     * Replication slot detail fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/replication-slot")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance replicationSlotFragment() {
        return buildReplicationSlotData(replicationSlotDetailFragment);
    }

    /**
     * Replication configuration and lag history fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/replication-config")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance replicationConfigFragment() {
        return buildReplicationConfigData(replicationConfigFragment);
    }

    /**
     * Replication danger zone fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/replication-danger")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance replicationDangerFragment() {
        return buildReplicationDangerData(replicationDangerFragment);
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /**
     * Builds the full dashboard page data, combining status, metrics,
     * breakdown, events, and sparklines.
     */
    private TemplateInstance buildFullDashboardData(Template template) {
        String status = connectorService.status().name();
        Duration uptime = Duration.between(connectorService.startTime(), Instant.now());
        String uptimeFormatted = String.format("%02d:%02d:%02d",
                uptime.toHours(), uptime.toMinutesPart(), uptime.toSecondsPart());

        return template
                // Status data
                .data("connectorId", connectorService.connectorId())
                .data("status", status)
                .data("statusColour", statusColour(status))
                .data("uptime", uptimeFormatted)
                .data("slotName", config.source().slotName())
                .data("publicationName", config.source().publicationName())
                .data("lastSnapshotTable", connectorService.lastSnapshotTable() != null
                        ? connectorService.lastSnapshotTable() : "N/A")
                .data("lastSnapshotStatus", connectorService.lastSnapshotStatus())
                // Metrics data
                .data("totalEvents", metricsService.totalEvents())
                .data("eventsPerSecond", metricsService.eventsPerSecondFormatted())
                .data("lastLsn", metricsService.lastLsnHex())
                .data("errorsCount", metricsService.errorsCount())
                // Sparklines for metric cards
                .data("eventsSparkline", SparklineGenerator.generate(
                        metricsService.eventsPerSecondHistory(), SPARK_SM_W, SPARK_SM_H, COLOUR_INDIGO))
                .data("eventsPerSecSparkline", SparklineGenerator.generate(
                        metricsService.eventsPerSecondHistory(), SPARK_SM_W, SPARK_SM_H, COLOUR_EMERALD))
                .data("errorsSparkline", SparklineGenerator.generate(
                        metricsService.errorRateHistory(), SPARK_SM_W, SPARK_SM_H, COLOUR_RED))
                // Breakdown data
                .data("totalInserts", metricsService.totalInserts())
                .data("totalUpdates", metricsService.totalUpdates())
                .data("totalDeletes", metricsService.totalDeletes())
                .data("totalSnapshotReads", metricsService.totalSnapshotReads())
                .data("insertPct", metricsService.percentage(metricsService.totalInserts()))
                .data("updatePct", metricsService.percentage(metricsService.totalUpdates()))
                .data("deletePct", metricsService.percentage(metricsService.totalDeletes()))
                .data("snapshotPct", metricsService.percentage(metricsService.totalSnapshotReads()))
                // Recent events
                .data("recentEvents", metricsService.recentEvents())
                // Errors
                .data("recentErrors", metricsService.recentErrors());
    }

    private TemplateInstance buildStatusData(Template template) {
        String status = connectorService.status().name();
        Duration uptime = Duration.between(connectorService.startTime(), Instant.now());
        String uptimeFormatted = String.format("%02d:%02d:%02d",
                uptime.toHours(), uptime.toMinutesPart(), uptime.toSecondsPart());

        return template
                .data("connectorId", connectorService.connectorId())
                .data("status", status)
                .data("statusColour", statusColour(status))
                .data("uptime", uptimeFormatted)
                .data("slotName", config.source().slotName())
                .data("publicationName", config.source().publicationName())
                .data("lastSnapshotTable", connectorService.lastSnapshotTable() != null
                        ? connectorService.lastSnapshotTable() : "N/A")
                .data("lastSnapshotStatus", connectorService.lastSnapshotStatus());
    }

    private TemplateInstance buildMetricsData(Template template) {
        return template
                .data("totalEvents", metricsService.totalEvents())
                .data("eventsPerSecond", metricsService.eventsPerSecondFormatted())
                .data("lastLsn", metricsService.lastLsnHex())
                .data("errorsCount", metricsService.errorsCount())
                .data("eventsSparkline", SparklineGenerator.generate(
                        metricsService.eventsPerSecondHistory(), SPARK_SM_W, SPARK_SM_H, COLOUR_INDIGO))
                .data("eventsPerSecSparkline", SparklineGenerator.generate(
                        metricsService.eventsPerSecondHistory(), SPARK_SM_W, SPARK_SM_H, COLOUR_EMERALD))
                .data("errorsSparkline", SparklineGenerator.generate(
                        metricsService.errorRateHistory(), SPARK_SM_W, SPARK_SM_H, COLOUR_RED));
    }

    private TemplateInstance buildBreakdownData(Template template) {
        return template
                .data("totalInserts", metricsService.totalInserts())
                .data("totalUpdates", metricsService.totalUpdates())
                .data("totalDeletes", metricsService.totalDeletes())
                .data("totalSnapshotReads", metricsService.totalSnapshotReads())
                .data("insertPct", metricsService.percentage(metricsService.totalInserts()))
                .data("updatePct", metricsService.percentage(metricsService.totalUpdates()))
                .data("deletePct", metricsService.percentage(metricsService.totalDeletes()))
                .data("snapshotPct", metricsService.percentage(metricsService.totalSnapshotReads()));
    }

    private TemplateInstance buildTablesData(Template template) {
        String pubName = config.source().publicationName();
        List<Map<String, Object>> tableList = new ArrayList<>();

        try (Connection conn = dataSource.getConnection()) {
            var metadataReader = new PostgresTableMetadataReader();
            List<TableId> tables = metadataReader.discoverPublicationTables(conn, pubName);

            for (TableId tableId : tables) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("canonicalName", tableId.canonicalName());
                try {
                    TableMetadata metadata = metadataReader.loadTableMetadata(conn, tableId);
                    entry.put("primaryKey", String.join(", ", metadata.primaryKeyColumns()));
                    entry.put("replicaIdentity", metadata.replicaIdentity());
                    entry.put("columnCount", metadata.columns().size());
                    entry.put("approximateRows", countRows(conn, tableId));
                    entry.put("valid", true);
                } catch (Exception e) {
                    entry.put("valid", false);
                    entry.put("error", e.getMessage());
                }
                tableList.add(entry);
            }
        } catch (Exception e) {
            // Database unavailable — show empty state
        }

        return template
                .data("publicationName", pubName)
                .data("tables", tableList);
    }

    private static long countRows(Connection conn, TableId tableId) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
                     "WHERE n.nspname = '" + tableId.schema() + "' AND c.relname = '" + tableId.table() + "'")) {
            if (rs.next()) return Math.max(0, rs.getLong(1));
        } catch (Exception ignored) {}
        return -1;
    }

    private static String statusColour(String status) {
        return switch (status) {
            case "RUNNING" -> "text-emerald-400";
            case "STARTING" -> "text-yellow-400";
            case "FAILED" -> "text-red-400";
            case "SNAPSHOTTING" -> "text-blue-400";
            default -> "text-gray-400";
        };
    }

    /**
     * Builds all data required for the full replication page.
     */
    private TemplateInstance buildReplicationData(Template template) {
        var slotHealth = replicationHealthService.getSlotHealth();
        var replicationStats = replicationHealthService.getReplicationStats();
        long lagBytes = slotHealth != null ? slotHealth.walLagBytes() : 0;
        boolean highLag = ReplicationHealthService.isHighLag(lagBytes);
        String lagColour = ReplicationHealthService.lagColour(lagBytes);
        String sparklineColour = highLag ? COLOUR_RED : COLOUR_EMERALD;

        return template
                .data("slotHealth", slotHealth)
                .data("replicationStats", replicationStats)
                .data("lagColour", lagColour)
                .data("highLag", highLag)
                .data("slotName", config.source().slotName())
                .data("walLagSparkline", SparklineGenerator.generate(
                        replicationHealthService.walLagHistory(), SPARK_SM_W, SPARK_SM_H, sparklineColour))
                .data("walLagLargeSparkline", SparklineGenerator.generate(
                        replicationHealthService.walLagHistory(), SPARK_LG_W, SPARK_LG_H, sparklineColour));
    }

    /**
     * Builds data for the replication health cards fragment.
     */
    private TemplateInstance buildReplicationHealthData(Template template) {
        var slotHealth = replicationHealthService.getSlotHealth();
        var replicationStats = replicationHealthService.getReplicationStats();
        long lagBytes = slotHealth != null ? slotHealth.walLagBytes() : 0;
        String sparklineColour = ReplicationHealthService.isHighLag(lagBytes) ? COLOUR_RED : COLOUR_EMERALD;

        return template
                .data("slotHealth", slotHealth)
                .data("replicationStats", replicationStats)
                .data("lagColour", ReplicationHealthService.lagColour(lagBytes))
                .data("walLagSparkline", SparklineGenerator.generate(
                        replicationHealthService.walLagHistory(), SPARK_SM_W, SPARK_SM_H, sparklineColour));
    }

    /**
     * Builds data for the replication slot detail fragment.
     */
    private TemplateInstance buildReplicationSlotData(Template template) {
        var slotHealth = replicationHealthService.getSlotHealth();
        long lagBytes = slotHealth != null ? slotHealth.walLagBytes() : 0;

        return template
                .data("slotHealth", slotHealth)
                .data("lagColour", ReplicationHealthService.lagColour(lagBytes))
                .data("slotName", config.source().slotName());
    }

    /**
     * Builds data for the replication config and lag history fragment.
     */
    private TemplateInstance buildReplicationConfigData(Template template) {
        var slotHealth = replicationHealthService.getSlotHealth();
        var replicationStats = replicationHealthService.getReplicationStats();
        long lagBytes = slotHealth != null ? slotHealth.walLagBytes() : 0;
        boolean highLag = ReplicationHealthService.isHighLag(lagBytes);
        String sparklineColour = highLag ? COLOUR_RED : COLOUR_EMERALD;

        return template
                .data("replicationStats", replicationStats)
                .data("highLag", highLag)
                .data("walLagLargeSparkline", SparklineGenerator.generate(
                        replicationHealthService.walLagHistory(), SPARK_LG_W, SPARK_LG_H, sparklineColour));
    }

    /**
     * Builds data for the replication danger zone fragment.
     */
    private TemplateInstance buildReplicationDangerData(Template template) {
        var slotHealth = replicationHealthService.getSlotHealth();
        long lagBytes = slotHealth != null ? slotHealth.walLagBytes() : 0;

        return template
                .data("slotHealth", slotHealth)
                .data("highLag", ReplicationHealthService.isHighLag(lagBytes))
                .data("slotName", config.source().slotName());
    }
}
