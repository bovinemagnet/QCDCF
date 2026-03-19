package com.paulsnow.qcdcf.runtime.dashboard;

import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import com.paulsnow.qcdcf.runtime.service.ConnectorService;
import com.paulsnow.qcdcf.runtime.service.MetricsService;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.Duration;
import java.time.Instant;

/**
 * Serves the HTMX-based operational dashboard.
 * <p>
 * All views are server-rendered HTML fragments suitable for HTMX consumption.
 * Minimal JavaScript — live updates via HTMX polling.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Path("/dashboard")
public class DashboardResource {

    @Inject
    Template dashboard;

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
    ConnectorService connectorService;

    @Inject
    MetricsService metricsService;

    @Inject
    ConnectorRuntimeConfig config;

    /**
     * Main dashboard page.
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance index() {
        return buildFullDashboardData(dashboard);
    }

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
     * Builds the full dashboard page data, combining status, metrics, breakdown, and events.
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
                .data("errorsCount", metricsService.errorsCount());
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

    private static String statusColour(String status) {
        return switch (status) {
            case "RUNNING" -> "text-emerald-400";
            case "STARTING" -> "text-yellow-400";
            case "FAILED" -> "text-red-400";
            case "SNAPSHOTTING" -> "text-blue-400";
            default -> "text-gray-400";
        };
    }
}
