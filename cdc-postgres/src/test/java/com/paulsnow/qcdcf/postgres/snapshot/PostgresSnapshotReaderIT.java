package com.paulsnow.qcdcf.postgres.snapshot;

import com.paulsnow.qcdcf.core.snapshot.SnapshotChunkPlan;
import com.paulsnow.qcdcf.model.CaptureMode;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.model.OperationType;
import com.paulsnow.qcdcf.model.RowKey;
import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.postgres.metadata.PostgresTableMetadataReader;
import com.paulsnow.qcdcf.postgres.metadata.TableMetadata;
import com.paulsnow.qcdcf.postgres.snapshot.PostgresSnapshotReader.SnapshotChunkReadResult;
import com.paulsnow.qcdcf.postgres.sql.PostgresChunkSqlBuilder;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link PostgresSnapshotReader}.
 * <p>
 * Exercises keyset-paginated snapshot chunk reads against a real PostgreSQL instance
 * via Testcontainers with a {@code snap_test.products} fixture table containing 10 rows.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Testcontainers
class PostgresSnapshotReaderIT {

    @Container
    static PostgreSQLContainer<?> postgres = PostgresContainerSupport.createContainer();

    private static final String SCHEMA = "snap_test";
    private static final String TABLE = "products";
    private static final String EMPTY_TABLE = "empty_products";
    private static final int PRODUCT_ROW_COUNT = 10;
    private static final String CONNECTOR_ID = "test-connector";

    private PostgresSnapshotReader reader;
    private PostgresTableMetadataReader metadataReader;
    private Connection connection;

    // -------------------------------------------------------------------------
    // Schema bootstrap — runs once per class; individual tests pay no DDL cost.
    // -------------------------------------------------------------------------

    @BeforeAll
    static void createTestSchema() throws SQLException {
        try (Connection conn = openConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);

            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS snap_test.products (
                        id    SERIAL PRIMARY KEY,
                        name  VARCHAR(100) NOT NULL,
                        price NUMERIC(10, 2) NOT NULL
                    )
                    """);

            // Insert exactly 10 rows with known, deterministic values so tests
            // can assert on specific column content.
            for (int i = 1; i <= PRODUCT_ROW_COUNT; i++) {
                stmt.execute(
                        "INSERT INTO snap_test.products (name, price) VALUES ('Product " + i + "', " + (i * 10) + ".00)"
                );
            }

            // A separate empty table to verify the zero-row edge case.
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS snap_test.empty_products (
                        id    SERIAL PRIMARY KEY,
                        name  VARCHAR(100) NOT NULL,
                        price NUMERIC(10, 2) NOT NULL
                    )
                    """);
        }
    }

    @BeforeEach
    void setUp() throws SQLException {
        metadataReader = new PostgresTableMetadataReader();
        reader = new PostgresSnapshotReader(new PostgresChunkSqlBuilder(), CONNECTOR_ID);
        connection = openConnection();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // -------------------------------------------------------------------------
    // Scenario 1 — Chunk size larger than table: all rows returned
    // -------------------------------------------------------------------------

    @Test
    void readChunk_chunkSizeExceedsTableSize_returnsAllTenRows() throws SQLException {
        TableMetadata metadata = loadProductsMetadata();
        SnapshotChunkPlan plan = firstChunkPlan(productsTableId(), 100);

        SnapshotChunkReadResult result = reader.readChunk(connection, plan, metadata);

        assertThat(result.events())
                .as("all 10 products should be returned when chunk size exceeds table size")
                .hasSize(PRODUCT_ROW_COUNT);
        assertThat(result.result().rowCount())
                .as("rowCount in chunk result should match number of events")
                .isEqualTo(PRODUCT_ROW_COUNT);
    }

    // -------------------------------------------------------------------------
    // Scenario 2 — Chunk size limits rows returned
    // -------------------------------------------------------------------------

    @Test
    void readChunk_chunkSizeThree_returnsExactlyThreeRows() throws SQLException {
        TableMetadata metadata = loadProductsMetadata();
        SnapshotChunkPlan plan = firstChunkPlan(productsTableId(), 3);

        SnapshotChunkReadResult result = reader.readChunk(connection, plan, metadata);

        assertThat(result.events())
                .as("chunk size 3 should limit the result to exactly 3 rows")
                .hasSize(3);
        assertThat(result.result().rowCount())
                .as("rowCount in chunk result should be 3")
                .isEqualTo(3);
    }

    // -------------------------------------------------------------------------
    // Scenario 3 — Rows are ordered by primary key
    // -------------------------------------------------------------------------

    @Test
    void readChunk_firstChunk_rowsAreOrderedByPrimaryKey() throws SQLException {
        TableMetadata metadata = loadProductsMetadata();
        SnapshotChunkPlan plan = firstChunkPlan(productsTableId(), 100);

        SnapshotChunkReadResult result = reader.readChunk(connection, plan, metadata);

        List<Integer> ids = extractIds(result.events());
        assertThat(ids)
                .as("rows must be in ascending primary key order")
                .isSorted();
    }

    // -------------------------------------------------------------------------
    // Scenario 4 — Resuming from last key skips already-read rows
    // -------------------------------------------------------------------------

    @Test
    void readChunk_withLowerBound_resumesAfterLastKeyOfPreviousChunk() throws SQLException {
        TableMetadata metadata = loadProductsMetadata();

        // Read the first chunk of 3 rows.
        SnapshotChunkPlan firstPlan = firstChunkPlan(productsTableId(), 3);
        SnapshotChunkReadResult firstResult = reader.readChunk(connection, firstPlan, metadata);
        RowKey lastKeyOfFirstChunk = firstResult.result().lastKey();

        int lastIdOfFirstChunk = idFromRowKey(lastKeyOfFirstChunk);

        // Read the second chunk starting after the first chunk's last key.
        SnapshotChunkPlan secondPlan = new SnapshotChunkPlan(
                productsTableId(), 1, 3, lastKeyOfFirstChunk, null);
        SnapshotChunkReadResult secondResult = reader.readChunk(connection, secondPlan, metadata);

        List<Integer> secondIds = extractIds(secondResult.events());
        assertThat(secondIds)
                .as("second chunk must not overlap with the first chunk")
                .allSatisfy(id -> assertThat(id).isGreaterThan(lastIdOfFirstChunk));
        assertThat(secondResult.events())
                .as("second chunk should also contain 3 rows given 10 total and chunk size 3")
                .hasSize(3);
    }

    // -------------------------------------------------------------------------
    // Scenario 5 — Iterating entire table in chunks covers all rows with no duplicates
    // -------------------------------------------------------------------------

    @Test
    void readChunk_iterateEntireTableInChunks_allRowsCoveredWithNoDuplicates() throws SQLException {
        TableMetadata metadata = loadProductsMetadata();
        TableId tableId = productsTableId();
        int chunkSize = 3;

        List<Integer> allIds = new ArrayList<>();
        RowKey lowerBound = null;
        int chunkIndex = 0;

        // Keep reading until a chunk returns fewer rows than the chunk size,
        // which signals that the end of the table has been reached.
        while (true) {
            SnapshotChunkPlan plan = new SnapshotChunkPlan(
                    tableId, chunkIndex, chunkSize, lowerBound, null);
            SnapshotChunkReadResult result = reader.readChunk(connection, plan, metadata);

            allIds.addAll(extractIds(result.events()));
            lowerBound = result.result().lastKey();
            chunkIndex++;

            if (result.events().size() < chunkSize) {
                break;
            }
        }

        assertThat(allIds)
                .as("all 10 product ids should be visited exactly once across all chunks")
                .hasSize(PRODUCT_ROW_COUNT)
                .doesNotHaveDuplicates();

        // Verify ids are globally ordered (each chunk is ordered, lowerBound pagination
        // ensures no overlap).
        assertThat(allIds)
                .as("ids collected in pagination order should be globally ascending")
                .isSorted();
    }

    // -------------------------------------------------------------------------
    // Scenario 6 — Events carry correct operation type and capture mode
    // -------------------------------------------------------------------------

    @Test
    void readChunk_allEvents_haveSnapshotReadOperationAndSnapshotCaptureMode() throws SQLException {
        TableMetadata metadata = loadProductsMetadata();
        SnapshotChunkPlan plan = firstChunkPlan(productsTableId(), 100);

        SnapshotChunkReadResult result = reader.readChunk(connection, plan, metadata);

        assertThat(result.events())
                .as("every snapshot event must have operation SNAPSHOT_READ")
                .allSatisfy(envelope ->
                        assertThat(envelope.operation())
                                .isEqualTo(OperationType.SNAPSHOT_READ));

        assertThat(result.events())
                .as("every snapshot event must have capture mode SNAPSHOT")
                .allSatisfy(envelope ->
                        assertThat(envelope.captureMode())
                                .isEqualTo(CaptureMode.SNAPSHOT));
    }

    // -------------------------------------------------------------------------
    // Scenario 7 — RowKey is populated from the primary key column
    // -------------------------------------------------------------------------

    @Test
    void readChunk_allEvents_rowKeyContainsIdColumn() throws SQLException {
        TableMetadata metadata = loadProductsMetadata();
        SnapshotChunkPlan plan = firstChunkPlan(productsTableId(), 100);

        SnapshotChunkReadResult result = reader.readChunk(connection, plan, metadata);

        assertThat(result.events())
                .as("every event's RowKey must contain an 'id' entry")
                .allSatisfy(envelope -> {
                    assertThat(envelope.key().columns())
                            .as("row key columns for event %s", envelope.eventId())
                            .containsKey("id");
                    assertThat(envelope.key().columns().get("id"))
                            .as("id value in row key must not be null")
                            .isNotNull();
                });
    }

    // -------------------------------------------------------------------------
    // Scenario 8 — Empty table returns zero rows and null lastKey
    // -------------------------------------------------------------------------

    @Test
    void readChunk_emptyTable_returnsZeroRowsAndNullLastKey() throws SQLException {
        TableMetadata metadata = metadataReader.loadTableMetadata(
                connection, new TableId(SCHEMA, EMPTY_TABLE));
        SnapshotChunkPlan plan = firstChunkPlan(new TableId(SCHEMA, EMPTY_TABLE), 100);

        SnapshotChunkReadResult result = reader.readChunk(connection, plan, metadata);

        assertThat(result.events())
                .as("empty table should produce no events")
                .isEmpty();
        assertThat(result.result().rowCount())
                .as("rowCount should be zero for an empty table")
                .isZero();
        assertThat(result.result().lastKey())
                .as("lastKey should be null when no rows were read")
                .isNull();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static Connection openConnection() throws SQLException {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword());
    }

    private TableId productsTableId() {
        return new TableId(SCHEMA, TABLE);
    }

    private TableMetadata loadProductsMetadata() throws SQLException {
        return metadataReader.loadTableMetadata(connection, productsTableId());
    }

    /**
     * Builds a {@link SnapshotChunkPlan} for the first chunk (no lower bound).
     */
    private static SnapshotChunkPlan firstChunkPlan(TableId tableId, int chunkSize) {
        return new SnapshotChunkPlan(tableId, 0, chunkSize, null, null);
    }

    /**
     * Extracts the {@code id} column value from each event's {@code after} map as an integer.
     */
    private static List<Integer> extractIds(List<ChangeEnvelope> events) {
        return events.stream()
                .map(e -> ((Number) e.after().get("id")).intValue())
                .collect(Collectors.toList());
    }

    /**
     * Extracts the {@code id} integer value from a {@link RowKey}.
     */
    private static int idFromRowKey(RowKey key) {
        return ((Number) key.columns().get("id")).intValue();
    }
}
