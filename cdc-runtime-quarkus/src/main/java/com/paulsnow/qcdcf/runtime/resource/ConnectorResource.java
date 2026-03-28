package com.paulsnow.qcdcf.runtime.resource;

import com.paulsnow.qcdcf.runtime.service.ConnectorService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.Map;

/**
 * REST resource for connector status and control.
 * <p>
 * Provides endpoints for querying status, pausing, resuming,
 * triggering snapshots, and viewing progress.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Path("/api/connector")
@Produces(MediaType.APPLICATION_JSON)
public class ConnectorResource {

    @Inject
    ConnectorService connectorService;

    /**
     * Return current connector status.
     */
    @GET
    @Path("/status")
    public Map<String, Object> status() {
        return Map.of(
                "connectorId", connectorService.connectorId(),
                "status", connectorService.status().name()
        );
    }

    /**
     * Pause the connector, stopping the WAL reader.
     */
    @POST
    @Path("/pause")
    public Map<String, Object> pause() {
        return connectorService.pause();
    }

    /**
     * Resume the connector, restarting the WAL reader.
     */
    @POST
    @Path("/resume")
    public Map<String, Object> resume() {
        return connectorService.resume();
    }

    /**
     * Trigger a table snapshot. Accepts JSON or form-encoded body with a {@code tableName} field.
     */
    @POST
    @Path("/snapshot")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED, MediaType.WILDCARD})
    public Map<String, Object> triggerSnapshot(Map<String, String> body) {
        String tableName = body != null ? body.getOrDefault("tableName", "unknown") : "unknown";
        return connectorService.triggerSnapshot(tableName);
    }

    /**
     * Return snapshot progress and general connector progress information.
     */
    @GET
    @Path("/progress")
    public Map<String, Object> progress() {
        return connectorService.getProgress();
    }
}
