package com.paulsnow.qcdcf.runtime.resource;

import com.paulsnow.qcdcf.runtime.service.ReplicationHealthService;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST resource exposing replication health data as JSON for programmatic access.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Path("/api/replication")
@Produces(MediaType.APPLICATION_JSON)
public class ReplicationResource {

    @Inject
    ReplicationHealthService replicationHealthService;

    /**
     * Returns replication slot health and general replication statistics as JSON.
     *
     * @return a map containing {@code slotHealth} and {@code replicationStats} keys
     */
    @GET
    @Path("/health")
    public Map<String, Object> health() {
        Map<String, Object> result = new LinkedHashMap<>();

        ReplicationHealthService.SlotHealth slotHealth = replicationHealthService.getSlotHealth();
        ReplicationHealthService.ReplicationStats stats = replicationHealthService.getReplicationStats();

        if (slotHealth != null) {
            Map<String, Object> slot = new LinkedHashMap<>();
            slot.put("slotName", slotHealth.slotName());
            slot.put("plugin", slotHealth.plugin());
            slot.put("slotType", slotHealth.slotType());
            slot.put("active", slotHealth.active());
            slot.put("confirmedFlushLsn", slotHealth.confirmedFlushLsnHex());
            slot.put("restartLsn", slotHealth.restartLsnHex());
            slot.put("walLagBytes", slotHealth.walLagBytes());
            slot.put("walLagFormatted", slotHealth.walLagFormatted());
            slot.put("catalogXmin", slotHealth.catalogXmin());
            slot.put("highLag", ReplicationHealthService.isHighLag(slotHealth.walLagBytes()));
            result.put("slotHealth", slot);
        } else {
            result.put("slotHealth", null);
        }

        if (stats != null) {
            Map<String, Object> repl = new LinkedHashMap<>();
            repl.put("walLevel", stats.walLevel());
            repl.put("currentWalLsn", stats.currentWalLsnHex());
            repl.put("maxReplicationSlots", stats.maxReplicationSlots());
            repl.put("usedReplicationSlots", stats.usedReplicationSlots());
            repl.put("maxWalSenders", stats.maxWalSenders());
            repl.put("activeWalSenders", stats.activeWalSenders());
            repl.put("walSegmentSize", stats.walSegmentSize());
            result.put("replicationStats", repl);
        } else {
            result.put("replicationStats", null);
        }

        return result;
    }
}
