package com.paulsnow.qcdcf.runtime.cli;

import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.postgres.metadata.PostgresTableMetadataReader;
import com.paulsnow.qcdcf.postgres.metadata.TableMetadata;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * REST endpoint that validates PostgreSQL prerequisites for CDC.
 * <p>
 * Checks connectivity, WAL level, replication slot, publication,
 * replica identity, and watermark table.
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
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
                rs.next();
                String walLevel = rs.getString(1);
                if ("logical".equals(walLevel)) {
                    checks.add(Map.of("check", "WAL level", "status", "OK", "value", walLevel));
                    passed++;
                } else {
                    checks.add(Map.of("check", "WAL level", "status", "FAIL", "value", walLevel, "expected", "logical"));
                    failed++;
                }
            }

            // 3. Replication slot
            String slotName = config.source().slotName();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT active FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
                if (rs.next()) {
                    checks.add(Map.of("check", "Replication slot '" + slotName + "'", "status", "OK",
                            "active", String.valueOf(rs.getBoolean("active"))));
                    passed++;
                } else {
                    checks.add(Map.of("check", "Replication slot '" + slotName + "'", "status", "WARN",
                            "detail", "Not found — will be created on start"));
                }
            }

            // 4. Publication
            String pubName = config.source().publicationName();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT 1 FROM pg_publication WHERE pubname = '" + pubName + "'")) {
                if (rs.next()) {
                    checks.add(Map.of("check", "Publication '" + pubName + "'", "status", "OK"));
                    passed++;
                } else {
                    checks.add(Map.of("check", "Publication '" + pubName + "'", "status", "FAIL",
                            "detail", "Not found — create with: CREATE PUBLICATION " + pubName + " FOR TABLE ..."));
                    failed++;
                }
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
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT 1 FROM information_schema.tables WHERE table_name = 'qcdcf_watermark'")) {
                if (rs.next()) {
                    checks.add(Map.of("check", "Watermark table", "status", "OK"));
                    passed++;
                } else {
                    checks.add(Map.of("check", "Watermark table", "status", "WARN",
                            "detail", "Not found — will be created on first snapshot"));
                }
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
