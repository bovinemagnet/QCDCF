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
 * Validates that PostgreSQL prerequisites are correctly configured for CDC.
 * <p>
 * Checks: database connectivity, logical replication settings, replication slot,
 * publication existence, and replica identity on published tables.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Command(
        name = "check",
        description = "Validate PostgreSQL prerequisites for CDC (replication, publication, replica identity)."
)
public class CheckCommand implements Runnable {

    @Inject
    DataSource dataSource;

    @Inject
    ConnectorRuntimeConfig config;

    @Override
    public void run() {
        System.out.println("QCDCF Prerequisite Check");
        System.out.println("========================");
        int passed = 0;
        int failed = 0;

        try (Connection conn = dataSource.getConnection()) {
            // 1. Database connectivity
            System.out.print("  Database connection ............ ");
            System.out.println("OK");
            passed++;

            // 2. WAL level
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
                rs.next();
                String walLevel = rs.getString(1);
                System.out.print("  WAL level ...................... ");
                if ("logical".equals(walLevel)) {
                    System.out.println("OK (" + walLevel + ")");
                    passed++;
                } else {
                    System.out.println("FAIL (expected 'logical', got '" + walLevel + "')");
                    failed++;
                }
            }

            // 3. Replication slot
            String slotName = config.source().slotName();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT active FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
                System.out.print("  Replication slot '" + slotName + "' .. ");
                if (rs.next()) {
                    boolean active = rs.getBoolean("active");
                    System.out.println("OK (exists, active=" + active + ")");
                    passed++;
                } else {
                    System.out.println("WARN (not found — will be created on start)");
                }
            }

            // 4. Publication
            String pubName = config.source().publicationName();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT 1 FROM pg_publication WHERE pubname = '" + pubName + "'")) {
                System.out.print("  Publication '" + pubName + "' ......... ");
                if (rs.next()) {
                    System.out.println("OK (exists)");
                    passed++;
                } else {
                    System.out.println("FAIL (not found — create with: CREATE PUBLICATION " + pubName + " FOR TABLE ...)");
                    failed++;
                }
            }

            // 5. Published tables and replica identity
            var metadataReader = new PostgresTableMetadataReader();
            List<TableId> tables = metadataReader.discoverPublicationTables(conn, pubName);
            System.out.print("  Published tables ............... ");
            if (tables.isEmpty()) {
                System.out.println("WARN (no tables in publication)");
            } else {
                System.out.println("OK (" + tables.size() + " tables)");
                passed++;
            }

            for (TableId tableId : tables) {
                try {
                    TableMetadata metadata = metadataReader.loadTableMetadata(conn, tableId);
                    System.out.printf("    %-30s PK=%s  identity=%s  OK%n",
                            tableId, metadata.primaryKeyColumns(), metadata.replicaIdentity());
                    passed++;
                } catch (Exception e) {
                    System.out.printf("    %-30s FAIL: %s%n", tableId, e.getMessage());
                    failed++;
                }
            }

            // 6. Watermark table
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT 1 FROM information_schema.tables WHERE table_name = 'qcdcf_watermark'")) {
                System.out.print("  Watermark table ................ ");
                if (rs.next()) {
                    System.out.println("OK (exists)");
                    passed++;
                } else {
                    System.out.println("WARN (not found — will be created on first snapshot)");
                }
            }

        } catch (Exception e) {
            System.out.println("FAIL (" + e.getMessage() + ")");
            failed++;
        }

        System.out.println();
        System.out.printf("Result: %d passed, %d failed%n", passed, failed);
        if (failed > 0) {
            System.out.println("Fix the issues above before starting the connector.");
        } else {
            System.out.println("All checks passed. Ready to start.");
        }
    }
}
