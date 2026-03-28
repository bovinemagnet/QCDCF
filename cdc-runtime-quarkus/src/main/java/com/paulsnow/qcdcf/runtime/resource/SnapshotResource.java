package com.paulsnow.qcdcf.runtime.resource;

import com.paulsnow.qcdcf.runtime.service.ConnectorService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST resource for snapshot operations.
 * <p>
 * Provides endpoints for triggering and querying table snapshots.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Path("/api/snapshots")
@Produces(MediaType.APPLICATION_JSON)
public class SnapshotResource {

    @Inject
    ConnectorService connectorService;

    /**
     * Trigger a snapshot for a specific table.
     * Accepts a JSON body with a {@code tableName} field.
     */
    @POST
    @Path("/trigger")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> trigger(Map<String, String> body) {
        String tableName = body != null ? body.getOrDefault("tableName", "unknown") : "unknown";
        return connectorService.triggerSnapshot(tableName);
    }

    /**
     * Return the current snapshot status.
     */
    @GET
    @Path("/status")
    public Map<String, Object> status() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connectorId", connectorService.connectorId());
        result.put("lastSnapshotTable", connectorService.lastSnapshotTable() != null
                ? connectorService.lastSnapshotTable() : "N/A");
        result.put("lastSnapshotStatus", connectorService.lastSnapshotStatus());
        return result;
    }
}
