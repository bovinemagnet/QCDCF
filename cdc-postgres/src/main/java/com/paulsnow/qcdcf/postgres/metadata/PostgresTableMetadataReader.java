package com.paulsnow.qcdcf.postgres.metadata;

import com.paulsnow.qcdcf.core.exception.InvalidTableException;
import com.paulsnow.qcdcf.core.exception.SourceReadException;
import com.paulsnow.qcdcf.model.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads table metadata from PostgreSQL system catalogues ({@code pg_catalog}).
 * <p>
 * Uses {@code pg_catalog} views directly rather than {@code information_schema}
 * for better performance and access to PostgreSQL-specific metadata such as
 * replica identity settings.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PostgresTableMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresTableMetadataReader.class);

    // Discover primary key columns in definition order
    private static final String PRIMARY_KEY_SQL = """
            SELECT a.attname AS column_name, a.attnum AS ordinal_position
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
            WHERE i.indisprimary
              AND n.nspname = ? AND c.relname = ?
            ORDER BY a.attnum
            """;

    // Read all column metadata from pg_attribute
    private static final String COLUMNS_SQL = """
            SELECT a.attname AS column_name,
                   a.attnum AS ordinal_position,
                   t.typname AS data_type,
                   a.attnotnull AS not_null,
                   a.atthasdef AS has_default
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_type t ON a.atttypid = t.oid
            WHERE n.nspname = ? AND c.relname = ?
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """;

    // Check replica identity setting
    private static final String REPLICA_IDENTITY_SQL = """
            SELECT c.relreplident
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ? AND c.relname = ?
            """;

    // Discover tables in a publication
    private static final String PUBLICATION_TABLES_SQL = """
            SELECT schemaname, tablename
            FROM pg_publication_tables
            WHERE pubname = ?
            """;

    private final ReplicaIdentityValidator validator;

    public PostgresTableMetadataReader() {
        this(new ReplicaIdentityValidator());
    }

    public PostgresTableMetadataReader(ReplicaIdentityValidator validator) {
        this.validator = validator;
    }

    /**
     * Loads and validates the metadata for the given table.
     *
     * @param connection the database connection
     * @param tableId    the table to read metadata for
     * @return the validated table metadata
     * @throws InvalidTableException if the table is not suitable for CDC
     * @throws SourceReadException   if metadata cannot be read
     */
    public TableMetadata loadTableMetadata(Connection connection, TableId tableId) {
        try {
            List<ColumnMetadata> columns = readColumns(connection, tableId);
            if (columns.isEmpty()) {
                throw new SourceReadException("Table not found or has no columns: " + tableId);
            }

            List<String> pkColumns = readPrimaryKeyColumns(connection, tableId);
            String replicaIdentity = readReplicaIdentity(connection, tableId);

            TableMetadata metadata = new TableMetadata(tableId, columns, pkColumns, replicaIdentity);

            // Validate the table is suitable for CDC
            validator.validate(metadata);

            LOG.info("Loaded metadata for {}: {} columns, PK={}, replica identity={}",
                    tableId, columns.size(), pkColumns, replicaIdentity);

            return metadata;
        } catch (InvalidTableException e) {
            throw e;  // re-throw validation errors as-is
        } catch (SQLException e) {
            throw new SourceReadException("Failed to read metadata for " + tableId, e);
        }
    }

    /**
     * Discovers all tables that are members of the given publication.
     *
     * @param connection      the database connection
     * @param publicationName the publication name
     * @return the list of table identifiers in the publication
     */
    public List<TableId> discoverPublicationTables(Connection connection, String publicationName) {
        List<TableId> tables = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(PUBLICATION_TABLES_SQL)) {
            ps.setString(1, publicationName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(new TableId(rs.getString("schemaname"), rs.getString("tablename")));
                }
            }
        } catch (SQLException e) {
            throw new SourceReadException("Failed to discover tables in publication: " + publicationName, e);
        }
        LOG.info("Publication '{}' contains {} tables: {}", publicationName, tables.size(), tables);
        return tables;
    }

    private List<String> readPrimaryKeyColumns(Connection connection, TableId tableId) throws SQLException {
        List<String> pkColumns = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(PRIMARY_KEY_SQL)) {
            ps.setString(1, tableId.schema());
            ps.setString(2, tableId.table());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    pkColumns.add(rs.getString("column_name"));
                }
            }
        }
        return pkColumns;
    }

    private List<ColumnMetadata> readColumns(Connection connection, TableId tableId) throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        List<String> pkColumns = readPrimaryKeyColumns(connection, tableId);

        try (PreparedStatement ps = connection.prepareStatement(COLUMNS_SQL)) {
            ps.setString(1, tableId.schema());
            ps.setString(2, tableId.table());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("column_name");
                    columns.add(new ColumnMetadata(
                            name,
                            rs.getString("data_type"),
                            rs.getInt("ordinal_position"),
                            !rs.getBoolean("not_null"),
                            pkColumns.contains(name)
                    ));
                }
            }
        }
        return columns;
    }

    private String readReplicaIdentity(Connection connection, TableId tableId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(REPLICA_IDENTITY_SQL)) {
            ps.setString(1, tableId.schema());
            ps.setString(2, tableId.table());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return switch (rs.getString("relreplident")) {
                        case "d" -> "DEFAULT";
                        case "f" -> "FULL";
                        case "i" -> "INDEX";
                        case "n" -> "NOTHING";
                        default -> "UNKNOWN";
                    };
                }
                return "UNKNOWN";
            }
        }
    }
}
