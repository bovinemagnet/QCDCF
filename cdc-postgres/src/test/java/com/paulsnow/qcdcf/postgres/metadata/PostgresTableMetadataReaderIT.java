package com.paulsnow.qcdcf.postgres.metadata;

import com.paulsnow.qcdcf.core.exception.InvalidTableException;
import com.paulsnow.qcdcf.core.exception.SourceReadException;
import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.testkit.postgres.PostgresContainerSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link PostgresTableMetadataReader}.
 * <p>
 * Exercises metadata discovery against a real PostgreSQL instance via
 * Testcontainers with logical replication enabled.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Testcontainers
class PostgresTableMetadataReaderIT {

    @Container
    static PostgreSQLContainer<?> postgres = PostgresContainerSupport.createContainer();

    private PostgresTableMetadataReader reader;
    private Connection connection;

    // -------------------------------------------------------------------------
    // Schema bootstrap — runs once, creates all fixtures so individual tests
    // are fast and do not pay per-test DDL overhead.
    // -------------------------------------------------------------------------

    @BeforeAll
    static void createTestSchema() throws SQLException {
        try (Connection conn = openConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE SCHEMA IF NOT EXISTS test_cdc");

            // Scenario 1 — single-column PK
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_cdc.single_pk (
                        id BIGINT PRIMARY KEY
                    )
                    """);

            // Scenario 2 — composite PK
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_cdc.composite_pk (
                        tenant_id  BIGINT NOT NULL,
                        record_id  BIGINT NOT NULL,
                        payload    TEXT,
                        PRIMARY KEY (tenant_id, record_id)
                    )
                    """);

            // Scenario 3 — rich column metadata (reused by multiple tests)
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_cdc.full_columns (
                        id          BIGINT PRIMARY KEY,
                        name        VARCHAR(255) NOT NULL,
                        description TEXT
                    )
                    """);

            // Scenario 4 — no primary key: validator must reject it
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_cdc.no_pk (
                        data TEXT
                    )
                    """);

            // Scenario 5 — NOTHING replica identity: validator must reject it
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_cdc.replica_nothing (
                        id BIGINT PRIMARY KEY
                    )
                    """);
            stmt.execute("ALTER TABLE test_cdc.replica_nothing REPLICA IDENTITY NOTHING");

            // Scenario 6 — FULL replica identity: must be accepted
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_cdc.replica_full (
                        id BIGINT PRIMARY KEY
                    )
                    """);
            stmt.execute("ALTER TABLE test_cdc.replica_full REPLICA IDENTITY FULL");

            // Scenario 7 — DEFAULT replica identity (standard table with PK):
            // no ALTER needed; DEFAULT is the PostgreSQL default.
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_cdc.replica_default (
                        id BIGINT PRIMARY KEY
                    )
                    """);

            // Scenario 8 — Publication membership
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_cdc.pub_table_alpha (
                        id BIGINT PRIMARY KEY
                    )
                    """);
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_cdc.pub_table_beta (
                        id BIGINT PRIMARY KEY
                    )
                    """);
            // The Testcontainer user is a superuser, so CREATE PUBLICATION succeeds.
            // Note: IF NOT EXISTS for publications requires PostgreSQL 17+; the
            // container runs PostgreSQL 16, so we use plain CREATE PUBLICATION.
            // The @BeforeAll runs once per test class so there is no duplicate risk.
            stmt.execute("""
                    CREATE PUBLICATION test_pub
                    FOR TABLE test_cdc.pub_table_alpha, test_cdc.pub_table_beta
                    """);

            // Scenario 10 — quoted / mixed-case identifiers
            // Double-quoting preserves case in PostgreSQL; the system catalogues
            // store the exact case-sensitive name.
            stmt.execute("CREATE SCHEMA IF NOT EXISTS \"TestCDC\"");
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS "TestCDC"."MixedCase" (
                        "Id" BIGINT PRIMARY KEY,
                        "Label" TEXT
                    )
                    """);
        }
    }

    @BeforeEach
    void setUp() throws SQLException {
        reader = new PostgresTableMetadataReader();
        connection = openConnection();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Connection openConnection() throws SQLException {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword());
    }

    // -------------------------------------------------------------------------
    // Scenario 1 — Single-column primary key discovery
    // -------------------------------------------------------------------------

    @Test
    void loadTableMetadata_singleColumnPk_pkColumnsContainsExactlyId() {
        var tableId = new TableId("test_cdc", "single_pk");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(metadata.primaryKeyColumns())
                .as("primary key columns for single_pk")
                .containsExactly("id");
    }

    // -------------------------------------------------------------------------
    // Scenario 2 — Composite primary key discovery
    // -------------------------------------------------------------------------

    @Test
    void loadTableMetadata_compositePk_pkColumnsContainsBothInDefinitionOrder() {
        var tableId = new TableId("test_cdc", "composite_pk");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(metadata.primaryKeyColumns())
                .as("primary key columns for composite_pk in definition order")
                .containsExactly("tenant_id", "record_id");
    }

    // -------------------------------------------------------------------------
    // Scenario 3 — All column metadata loaded
    // -------------------------------------------------------------------------

    @Test
    void loadTableMetadata_richColumnTable_allColumnNamesPresent() {
        var tableId = new TableId("test_cdc", "full_columns");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(metadata.columns())
                .extracting(ColumnMetadata::name)
                .as("column names in ordinal order")
                .containsExactly("id", "name", "description");
    }

    @Test
    void loadTableMetadata_richColumnTable_dataTypesAreMappedCorrectly() {
        var tableId = new TableId("test_cdc", "full_columns");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        // PostgreSQL pg_type.typname values for the DDL types used above
        assertThat(columnByName(metadata, "id").dataType())
                .as("id column data type")
                .isEqualTo("int8");          // BIGINT → int8 in pg_type

        assertThat(columnByName(metadata, "name").dataType())
                .as("name column data type")
                .isEqualTo("varchar");       // VARCHAR → varchar in pg_type

        assertThat(columnByName(metadata, "description").dataType())
                .as("description column data type")
                .isEqualTo("text");
    }

    @Test
    void loadTableMetadata_richColumnTable_ordinalPositionsAreOneBased() {
        var tableId = new TableId("test_cdc", "full_columns");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(columnByName(metadata, "id").ordinalPosition()).isEqualTo(1);
        assertThat(columnByName(metadata, "name").ordinalPosition()).isEqualTo(2);
        assertThat(columnByName(metadata, "description").ordinalPosition()).isEqualTo(3);
    }

    @Test
    void loadTableMetadata_richColumnTable_nullabilityReflectsDdlConstraints() {
        var tableId = new TableId("test_cdc", "full_columns");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        // id is PK (implicitly NOT NULL), name is explicitly NOT NULL, description is nullable
        assertThat(columnByName(metadata, "id").nullable())
                .as("id is NOT NULL via PK constraint")
                .isFalse();
        assertThat(columnByName(metadata, "name").nullable())
                .as("name is explicitly NOT NULL")
                .isFalse();
        assertThat(columnByName(metadata, "description").nullable())
                .as("description allows NULLs")
                .isTrue();
    }

    @Test
    void loadTableMetadata_richColumnTable_primaryKeyFlagSetOnlyForPkColumn() {
        var tableId = new TableId("test_cdc", "full_columns");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(columnByName(metadata, "id").primaryKey())
                .as("id is the primary key column")
                .isTrue();
        assertThat(columnByName(metadata, "name").primaryKey())
                .as("name is not a primary key column")
                .isFalse();
        assertThat(columnByName(metadata, "description").primaryKey())
                .as("description is not a primary key column")
                .isFalse();
    }

    // -------------------------------------------------------------------------
    // Scenario 4 — Table without primary key is rejected
    // -------------------------------------------------------------------------

    @Test
    void loadTableMetadata_tableWithNoPrimaryKey_throwsInvalidTableExceptionMentioningNoPrimaryKey() {
        var tableId = new TableId("test_cdc", "no_pk");

        assertThatThrownBy(() -> reader.loadTableMetadata(connection, tableId))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("no primary key");
    }

    // -------------------------------------------------------------------------
    // Scenario 5 — NOTHING replica identity is rejected
    // -------------------------------------------------------------------------

    @Test
    void loadTableMetadata_nothingReplicaIdentity_throwsInvalidTableException() {
        var tableId = new TableId("test_cdc", "replica_nothing");

        assertThatThrownBy(() -> reader.loadTableMetadata(connection, tableId))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("NOTHING");
    }

    // -------------------------------------------------------------------------
    // Scenario 6 — FULL replica identity is accepted
    // -------------------------------------------------------------------------

    @Test
    void loadTableMetadata_fullReplicaIdentity_loadsSuccessfullyWithReplicaIdentityFull() {
        var tableId = new TableId("test_cdc", "replica_full");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(metadata.replicaIdentity())
                .as("replica identity for replica_full")
                .isEqualTo("FULL");
    }

    // -------------------------------------------------------------------------
    // Scenario 7 — DEFAULT replica identity is accepted
    // -------------------------------------------------------------------------

    @Test
    void loadTableMetadata_defaultReplicaIdentity_loadsSuccessfullyWithReplicaIdentityDefault() {
        var tableId = new TableId("test_cdc", "replica_default");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(metadata.replicaIdentity())
                .as("replica identity for replica_default")
                .isEqualTo("DEFAULT");
    }

    // -------------------------------------------------------------------------
    // Scenario 8 — Publication membership discovery
    // -------------------------------------------------------------------------

    @Test
    void discoverPublicationTables_knownPublication_returnsAllMemberTables() {
        List<TableId> tables = reader.discoverPublicationTables(connection, "test_pub");

        assertThat(tables)
                .as("tables in test_pub publication")
                .hasSize(2)
                .extracting(TableId::table)
                .containsExactlyInAnyOrder("pub_table_alpha", "pub_table_beta");
    }

    @Test
    void discoverPublicationTables_knownPublication_allTablesHaveCorrectSchema() {
        List<TableId> tables = reader.discoverPublicationTables(connection, "test_pub");

        assertThat(tables)
                .extracting(TableId::schema)
                .as("all publication tables belong to test_cdc schema")
                .containsOnly("test_cdc");
    }

    @Test
    void discoverPublicationTables_nonExistentPublication_returnsEmptyList() {
        // pg_publication_tables simply returns no rows for an unknown publication name;
        // this is not an error condition in the current implementation.
        List<TableId> tables = reader.discoverPublicationTables(connection, "no_such_publication");

        assertThat(tables)
                .as("unknown publication yields empty result")
                .isEmpty();
    }

    // -------------------------------------------------------------------------
    // Scenario 9 — Non-existent table throws SourceReadException
    // -------------------------------------------------------------------------

    @Test
    void loadTableMetadata_nonExistentTable_throwsSourceReadException() {
        var tableId = new TableId("test_cdc", "does_not_exist");

        assertThatThrownBy(() -> reader.loadTableMetadata(connection, tableId))
                .isInstanceOf(SourceReadException.class)
                .hasMessageContaining("does_not_exist");
    }

    @Test
    void loadTableMetadata_nonExistentSchema_throwsSourceReadException() {
        var tableId = new TableId("no_such_schema", "some_table");

        assertThatThrownBy(() -> reader.loadTableMetadata(connection, tableId))
                .isInstanceOf(SourceReadException.class);
    }

    // -------------------------------------------------------------------------
    // Scenario 10 — Quoted / mixed-case identifiers
    // -------------------------------------------------------------------------

    @Test
    void loadTableMetadata_mixedCaseSchemaAndTable_readsMetadataSuccessfully() {
        // PostgreSQL stores the exact case-sensitive name when the identifier was quoted at DDL time.
        // TableId must use the same case as was quoted in the CREATE TABLE statement.
        var tableId = new TableId("TestCDC", "MixedCase");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(metadata.tableId()).isEqualTo(tableId);
    }

    @Test
    void loadTableMetadata_mixedCaseTable_pkColumnNamePreservesCase() {
        var tableId = new TableId("TestCDC", "MixedCase");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(metadata.primaryKeyColumns())
                .as("primary key column preserves quoted case")
                .containsExactly("Id");
    }

    @Test
    void loadTableMetadata_mixedCaseTable_columnNamesPreserveCase() {
        var tableId = new TableId("TestCDC", "MixedCase");

        TableMetadata metadata = reader.loadTableMetadata(connection, tableId);

        assertThat(metadata.columns())
                .extracting(ColumnMetadata::name)
                .as("column names preserve quoted case")
                .containsExactly("Id", "Label");
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Finds a column by name in the metadata, failing fast if not present.
     */
    private static ColumnMetadata columnByName(TableMetadata metadata, String name) {
        return metadata.columns().stream()
                .filter(c -> c.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Column '" + name + "' not found in metadata"));
    }
}
