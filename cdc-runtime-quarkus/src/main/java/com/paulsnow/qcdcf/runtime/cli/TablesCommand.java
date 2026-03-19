package com.paulsnow.qcdcf.runtime.cli;

import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.postgres.metadata.PostgresTableMetadataReader;
import com.paulsnow.qcdcf.postgres.metadata.TableMetadata;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import jakarta.inject.Inject;
import picocli.CommandLine.Command;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * Lists tables monitored by the configured publication.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Command(
        name = "tables",
        description = "List tables in the configured publication with metadata."
)
public class TablesCommand implements Runnable {

    @Inject
    DataSource dataSource;

    @Inject
    ConnectorRuntimeConfig config;

    @Override
    public void run() {
        String pubName = config.source().publicationName();
        System.out.println("Tables in publication '" + pubName + "':");
        System.out.println();

        try (Connection conn = dataSource.getConnection()) {
            var metadataReader = new PostgresTableMetadataReader();
            List<TableId> tables = metadataReader.discoverPublicationTables(conn, pubName);

            if (tables.isEmpty()) {
                System.out.println("  (no tables found)");
                return;
            }

            System.out.printf("  %-35s %-20s %-15s %-10s%n", "TABLE", "PRIMARY KEY", "IDENTITY", "COLUMNS");
            System.out.printf("  %-35s %-20s %-15s %-10s%n", "-----", "-----------", "--------", "-------");

            for (TableId tableId : tables) {
                try {
                    TableMetadata metadata = metadataReader.loadTableMetadata(conn, tableId);
                    long rowCount = countRows(conn, tableId);
                    System.out.printf("  %-35s %-20s %-15s %-10d%n",
                            tableId.canonicalName(),
                            String.join(", ", metadata.primaryKeyColumns()),
                            metadata.replicaIdentity(),
                            metadata.columns().size());
                    System.out.printf("    Approximate rows: %d%n", rowCount);
                } catch (Exception e) {
                    System.out.printf("  %-35s ERROR: %s%n", tableId.canonicalName(), e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to list tables: " + e.getMessage());
        }
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
