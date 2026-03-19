package com.paulsnow.qcdcf.runtime.dashboard;

import com.paulsnow.qcdcf.runtime.service.ConnectorService;
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
    ConnectorService connectorService;

    /**
     * Main dashboard page.
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance index() {
        return buildDashboardData(dashboard);
    }

    /**
     * Status fragment for HTMX polling.
     */
    @GET
    @Path("/fragments/status")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance statusFragment() {
        return buildDashboardData(statusFragment);
    }

    private TemplateInstance buildDashboardData(Template template) {
        String status = connectorService.status().name();
        Duration uptime = Duration.between(connectorService.startTime(), Instant.now());
        String uptimeFormatted = String.format("%02d:%02d:%02d",
                uptime.toHours(), uptime.toMinutesPart(), uptime.toSecondsPart());

        return template
                .data("connectorId", connectorService.connectorId())
                .data("status", status)
                .data("statusColour", statusColour(status))
                .data("uptime", uptimeFormatted)
                .data("lastSnapshotTable", connectorService.lastSnapshotTable() != null
                        ? connectorService.lastSnapshotTable() : "N/A")
                .data("lastSnapshotStatus", connectorService.lastSnapshotStatus());
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
