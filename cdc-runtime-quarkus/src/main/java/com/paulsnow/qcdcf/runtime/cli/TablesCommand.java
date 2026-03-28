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
 * REST endpoint listing tables in the configured publication with metadata.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Path("/api/cli")
@Produces(MediaType.APPLICATION_JSON)
public class TablesCommand {

    @Inject
    DataSource dataSource;

    @Inject
    ConnectorRuntimeConfig config;

    @GET
    @Path("/tables")
    public Map<String, Object> tables() {
        String pubName = config.source().publicationName();
        List<Map<String, Object>> tableList = new ArrayList<>();

        try (Connection conn = dataSource.getConnection()) {
            var metadataReader = new PostgresTableMetadataReader();
            List<TableId> tables = metadataReader.discoverPublicationTables(conn, pubName);

            for (TableId tableId : tables) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("schema", tableId.schema());
                entry.put("table", tableId.table());
                entry.put("canonicalName", tableId.canonicalName());
                try {
                    TableMetadata metadata = metadataReader.loadTableMetadata(conn, tableId);
                    entry.put("primaryKey", metadata.primaryKeyColumns());
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
            return Map.of("publication", pubName, "error", e.getMessage());
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("publication", pubName);
        result.put("tableCount", tableList.size());
        result.put("tables", tableList);
        return result;
    }

    private long countRows(Connection conn, TableId tableId) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
                     "WHERE n.nspname = '" + tableId.schema() + "' AND c.relname = '" + tableId.table() + "'")) {
            if (rs.next()) return Math.max(0, rs.getLong(1));
        } catch (Exception ignored) {}
        return -1;
    }
}
