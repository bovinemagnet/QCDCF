package com.paulsnow.qcdcf.runtime.cli;

import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.postgres.metadata.PostgresTableMetadataReader;
import com.paulsnow.qcdcf.postgres.metadata.TableMetadata;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import com.paulsnow.qcdcf.runtime.service.ConnectorValidator;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * REST endpoint that validates PostgreSQL prerequisites for CDC.
 * <p>
 * Checks connectivity, WAL level, replication slot, publication,
 * replica identity, and watermark table. Delegates parameterised
 * queries to {@link ConnectorValidator}.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Path("/api/cli")
@Produces(MediaType.APPLICATION_JSON)
public class CheckCommand {

    @Inject
    DataSource dataSource;

    @Inject
    ConnectorRuntimeConfig config;

    @Inject
    ConnectorValidator validator;

    @GET
    @Path("/check")
    public Map<String, Object> check() {
        List<Map<String, String>> checks = new ArrayList<>();
        int passed = 0;
        int failed = 0;

        try (Connection conn = dataSource.getConnection()) {
            // 1. Connectivity
            checks.add(Map.of("check", "Database connection", "status", "OK"));
            passed++;

            // 2. WAL level
            var walResults = validator.validateWalLevel(conn);
            checks.addAll(walResults);
            for (var r : walResults) {
                if ("OK".equals(r.get("status"))) passed++;
                else if ("FAIL".equals(r.get("status"))) failed++;
            }

            // 3. Replication slot
            String slotName = config.source().slotName();
            var slotResults = validator.validateSlot(conn, slotName);
            checks.addAll(slotResults);
            for (var r : slotResults) {
                if ("OK".equals(r.get("status"))) passed++;
                else if ("FAIL".equals(r.get("status"))) failed++;
            }

            // 4. Publication
            String pubName = config.source().publicationName();
            var pubResults = validator.validatePublication(conn, pubName);
            checks.addAll(pubResults);
            for (var r : pubResults) {
                if ("OK".equals(r.get("status"))) passed++;
                else if ("FAIL".equals(r.get("status"))) failed++;
            }

            // 5. Published tables
            var metadataReader = new PostgresTableMetadataReader();
            List<TableId> tables = metadataReader.discoverPublicationTables(conn, pubName);
            if (tables.isEmpty()) {
                checks.add(Map.of("check", "Published tables", "status", "WARN", "detail", "No tables in publication"));
            } else {
                checks.add(Map.of("check", "Published tables", "status", "OK", "count", String.valueOf(tables.size())));
                passed++;
                for (TableId tableId : tables) {
                    try {
                        TableMetadata metadata = metadataReader.loadTableMetadata(conn, tableId);
                        checks.add(Map.of("check", "Table " + tableId, "status", "OK",
                                "pk", String.join(", ", metadata.primaryKeyColumns()),
                                "identity", metadata.replicaIdentity()));
                        passed++;
                    } catch (Exception e) {
                        checks.add(Map.of("check", "Table " + tableId, "status", "FAIL", "detail", e.getMessage()));
                        failed++;
                    }
                }
            }

            // 6. Watermark table
            var watermarkResults = validator.validateWatermarkTable(conn);
            checks.addAll(watermarkResults);
            for (var r : watermarkResults) {
                if ("OK".equals(r.get("status"))) passed++;
                else if ("FAIL".equals(r.get("status"))) failed++;
            }

        } catch (Exception e) {
            checks.add(Map.of("check", "Database connection", "status", "FAIL", "detail", e.getMessage()));
            failed++;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("passed", passed);
        result.put("failed", failed);
        result.put("ready", failed == 0);
        result.put("checks", checks);
        return result;
    }
}
