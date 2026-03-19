package com.paulsnow.qcdcf.postgres.snapshot;

import com.paulsnow.qcdcf.core.reconcile.DefaultReconciliationEngine;
import com.paulsnow.qcdcf.core.reconcile.ReconciliationResult;
import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.core.snapshot.DefaultChunkPlanner;
import com.paulsnow.qcdcf.core.snapshot.DefaultSnapshotCoordinator;
import com.paulsnow.qcdcf.core.snapshot.SnapshotOptions;
import com.paulsnow.qcdcf.core.watermark.WatermarkAwareEventRouter;
import com.paulsnow.qcdcf.core.watermark.WatermarkWindow;
import com.paulsnow.qcdcf.model.*;
import com.paulsnow.qcdcf.postgres.metadata.PostgresTableMetadataReader;
import com.paulsnow.qcdcf.postgres.metadata.TableMetadata;
import com.paulsnow.qcdcf.postgres.replication.*;
import com.paulsnow.qcdcf.postgres.sql.PostgresChunkSqlBuilder;
import com.paulsnow.qcdcf.postgres.watermark.PostgresWatermarkWriter;
import com.paulsnow.qcdcf.testkit.postgres.PostgresContainerSupport;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving that watermark events appear in the WAL stream
 * during a snapshot and that the full reconciliation pipeline works end-to-end.
 * <p>
 * This is the most important integration test in the project — it validates the
 * core Netflix DBLog pattern: watermark brackets → snapshot chunk → reconciliation.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WatermarkSnapshotIT {

    private static final String SCHEMA = "snap_test";
    private static final String TABLE = "products";
    private static final String WATERMARK_TABLE = "qcdcf_watermark";
    private static final String PUBLICATION = "snap_test_pub";
    private static final String SLOT = "snap_test_slot";
    private static final TableId TABLE_ID = new TableId(SCHEMA, TABLE);
    private static final TableId WATERMARK_TABLE_ID = new TableId("public", WATERMARK_TABLE);

    @Container
    static final PostgreSQLContainer<?> postgres = PostgresContainerSupport.createContainer();

    // Shared state
    private static final CopyOnWriteArrayList<ChangeEnvelope> allWalEvents = new CopyOnWriteArrayList<>();
    private static PostgresLogicalReplicationClient walClient;
    private static PostgresLogStreamReader walReader;
    private static Thread walReaderThread;

    @BeforeAll
    static void setupDatabaseAndPipeline() throws Exception {
        try (Connection conn = openConnection(); Statement stmt = conn.createStatement()) {
            // Create schema and table
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            stmt.execute("""
                    CREATE TABLE snap_test.products (
                        id    SERIAL PRIMARY KEY,
                        name  VARCHAR(255) NOT NULL,
                        price NUMERIC(10,2) NOT NULL
                    )
                    """);

            // Insert initial data (10 rows)
            for (int i = 1; i <= 10; i++) {
                stmt.execute("INSERT INTO snap_test.products (name, price) VALUES ('Product " + i + "', " + (i * 10.0) + ")");
            }

            // Create watermark table in public schema (must be in publication)
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS qcdcf_watermark (
                        id           BIGSERIAL PRIMARY KEY,
                        window_id    UUID NOT NULL,
                        boundary     VARCHAR(4) NOT NULL,
                        table_schema VARCHAR(255) NOT NULL,
                        table_name   VARCHAR(255) NOT NULL,
                        chunk_index  INTEGER NOT NULL,
                        created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                    )
                    """);

            // Publication includes BOTH the data table AND the watermark table
            // This is critical: watermark writes must appear in the WAL stream
            stmt.execute("CREATE PUBLICATION " + PUBLICATION
                    + " FOR TABLE " + SCHEMA + "." + TABLE + ", public." + WATERMARK_TABLE);
        }

        // Create replication slot
        walClient = new PostgresLogicalReplicationClient(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(),
                SLOT, PUBLICATION);
        try (Connection conn = openConnection()) {
            walClient.createSlotIfNotExists(conn);
        }

        // Start WAL reader capturing ALL events (including watermark table events)
        PgOutputMessageDecoder decoder = new PgOutputMessageDecoder();
        PgOutputEventNormaliser normaliser = new PgOutputEventNormaliser("test-connector");

        EventSink captureSink = new EventSink() {
            @Override
            public PublishResult publish(ChangeEnvelope event) {
                allWalEvents.add(event);
                return new PublishResult.Success(1);
            }
            @Override
            public PublishResult publishBatch(List<ChangeEnvelope> events) {
                allWalEvents.addAll(events);
                return new PublishResult.Success(events.size());
            }
        };

        walReader = new PostgresLogStreamReader(walClient, decoder, normaliser, captureSink);
        walReaderThread = new Thread(() -> walReader.start(0L), "watermark-snapshot-it-reader");
        walReaderThread.setDaemon(true);
        walReaderThread.start();

        Thread.sleep(2_000); // Wait for stream to establish
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (walReader != null) walReader.stop();
        if (walReaderThread != null) walReaderThread.join(5_000);
        if (walClient != null) {
            walClient.close();
            try (Connection conn = openConnection()) {
                walClient.dropSlot(conn);
            } catch (Exception ignored) {}
        }
    }

    // ── Test 1: Watermark writes appear in WAL ──────────────────────────

    @Test
    @Order(1)
    void watermarkWritesAppearInWalStream() throws Exception {
        allWalEvents.clear();
        Thread.sleep(500);

        // Write a low watermark
        try (Connection conn = openConnection()) {
            var writer = new PostgresWatermarkWriter(conn);
            WatermarkWindow window = writer.openWindow(TABLE_ID, 0);

            // Wait for WAL event to propagate
            waitForEventsFromTable(WATERMARK_TABLE_ID, 1, Duration.ofSeconds(10));

            // Verify a watermark INSERT appeared in the WAL
            List<ChangeEnvelope> watermarkEvents = allWalEvents.stream()
                    .filter(e -> e.tableId().equals(WATERMARK_TABLE_ID))
                    .toList();

            assertThat(watermarkEvents)
                    .as("Watermark LOW write should appear as an INSERT in the WAL stream")
                    .hasSizeGreaterThanOrEqualTo(1);

            ChangeEnvelope lowEvent = watermarkEvents.getFirst();
            assertThat(lowEvent.operation()).isEqualTo(OperationType.INSERT);
            assertThat(lowEvent.after().get("boundary")).isEqualTo("LOW");

            // Write the high watermark
            writer.closeWindow(window);
            waitForEventsFromTable(WATERMARK_TABLE_ID, 2, Duration.ofSeconds(10));

            watermarkEvents = allWalEvents.stream()
                    .filter(e -> e.tableId().equals(WATERMARK_TABLE_ID))
                    .toList();

            assertThat(watermarkEvents)
                    .as("Both LOW and HIGH watermark writes should appear in WAL")
                    .hasSizeGreaterThanOrEqualTo(2);

            ChangeEnvelope highEvent = watermarkEvents.get(watermarkEvents.size() - 1);
            assertThat(highEvent.after().get("boundary")).isEqualTo("HIGH");
        }
    }

    // ── Test 2: Snapshot chunk reads correct data ────────────────────────

    @Test
    @Order(2)
    void snapshotChunkReadsCorrectData() throws Exception {
        try (Connection conn = openConnection()) {
            var metadataReader = new PostgresTableMetadataReader();
            TableMetadata metadata = metadataReader.loadTableMetadata(conn, TABLE_ID);
            var sqlBuilder = new PostgresChunkSqlBuilder();
            var snapshotReader = new PostgresSnapshotReader(sqlBuilder, "test-connector");

            // Read first chunk (size 5 of 10 rows)
            var plan = new com.paulsnow.qcdcf.core.snapshot.SnapshotChunkPlan(TABLE_ID, 0, 5, null, null);
            PostgresSnapshotReader.SnapshotChunkReadResult result = snapshotReader.readChunk(conn, plan, metadata);

            assertThat(result.events()).hasSize(5);
            assertThat(result.result().rowCount()).isEqualTo(5);
            assertThat(result.events()).allMatch(e -> e.operation() == OperationType.SNAPSHOT_READ);
            assertThat(result.events()).allMatch(e -> e.captureMode() == CaptureMode.SNAPSHOT);

            // Read second chunk using last key
            var plan2 = new com.paulsnow.qcdcf.core.snapshot.SnapshotChunkPlan(
                    TABLE_ID, 1, 5, result.result().lastKey(), null);
            PostgresSnapshotReader.SnapshotChunkReadResult result2 = snapshotReader.readChunk(conn, plan2, metadata);

            assertThat(result2.events()).hasSize(5);

            // Total should be 10 rows
            assertThat(result.events().size() + result2.events().size()).isEqualTo(10);
        }
    }

    // ── Test 3: Full snapshot with watermark bracketing ──────────────────

    @Test
    @Order(3)
    void fullSnapshotWithWatermarkBracketingProducesReconcilableOutput() throws Exception {
        allWalEvents.clear();
        Thread.sleep(500);

        List<ChangeEnvelope> snapshotOutput = new ArrayList<>();

        try (Connection conn = openConnection()) {
            var metadataReader = new PostgresTableMetadataReader();
            TableMetadata metadata = metadataReader.loadTableMetadata(conn, TABLE_ID);
            var sqlBuilder = new PostgresChunkSqlBuilder();
            var snapshotReader = new PostgresSnapshotReader(sqlBuilder, "test-connector");
            var watermarkWriter = new PostgresWatermarkWriter(conn);

            var coordinator = new DefaultSnapshotCoordinator(
                    watermarkWriter,
                    new DefaultChunkPlanner(),
                    plan -> {
                        PostgresSnapshotReader.SnapshotChunkReadResult readResult =
                                snapshotReader.readChunk(conn, plan, metadata);
                        return new DefaultSnapshotCoordinator.ChunkReader.ChunkReadResult(
                                readResult.events(), readResult.result());
                    },
                    (events, chunkResult) -> snapshotOutput.addAll(events)
            );

            long totalRows = coordinator.triggerSnapshot(TABLE_ID, new SnapshotOptions(TABLE_ID, 5));

            assertThat(totalRows).isEqualTo(10);
            assertThat(snapshotOutput).hasSize(10);

            // All snapshot events should have watermark context
            assertThat(snapshotOutput)
                    .as("Every snapshot event should have a watermark context")
                    .allMatch(e -> e.watermark() != null);

            assertThat(snapshotOutput)
                    .as("Every watermark context should be closed (low + high marks set)")
                    .allMatch(e -> e.watermark().isClosed());
        }

        // Wait for watermark events to appear in WAL
        waitForEventsFromTable(WATERMARK_TABLE_ID, 4, Duration.ofSeconds(10));

        // Verify watermark events in WAL (2 chunks × 2 boundaries = 4 watermark events)
        List<ChangeEnvelope> watermarkWalEvents = allWalEvents.stream()
                .filter(e -> e.tableId().equals(WATERMARK_TABLE_ID))
                .toList();

        assertThat(watermarkWalEvents)
                .as("2 chunks should produce 4 watermark events (LOW+HIGH for each)")
                .hasSizeGreaterThanOrEqualTo(4);

        // Verify LOW/HIGH ordering: should alternate LOW, HIGH, LOW, HIGH
        List<String> boundaries = watermarkWalEvents.stream()
                .map(e -> (String) e.after().get("boundary"))
                .toList();

        assertThat(boundaries.getFirst()).isEqualTo("LOW");
        assertThat(boundaries.get(1)).isEqualTo("HIGH");
    }

    // ── Test 4: Reconciliation with concurrent WAL changes ──────────────

    @Test
    @Order(4)
    void reconciliationHandlesConcurrentWalChangeDuringSnapshot() throws Exception {
        allWalEvents.clear();
        Thread.sleep(500);

        // We'll simulate what happens when a row is updated while a snapshot is in progress:
        // 1. Write LOW watermark
        // 2. Read snapshot chunk (gets product id=1 with old value)
        // 3. UPDATE product id=1 (this WAL event arrives between LOW and HIGH)
        // 4. Write HIGH watermark
        // 5. Reconciliation should drop the LOG event for id=1 (snapshot has consistent data)

        try (Connection conn = openConnection()) {
            var watermarkWriter = new PostgresWatermarkWriter(conn);

            // 1. Open window
            WatermarkWindow window = watermarkWriter.openWindow(TABLE_ID, 99);

            // 2. Read snapshot chunk (simulated — just create snapshot events)
            List<ChangeEnvelope> snapshotEvents = List.of(
                    createSnapshotEvent(1, "Product 1", 10.0),
                    createSnapshotEvent(2, "Product 2", 20.0)
            );

            // 3. Concurrent UPDATE while window is open
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("UPDATE snap_test.products SET name = 'Updated Product 1', price = 99.99 WHERE id = 1");
            }

            // Wait for the UPDATE to appear in WAL
            Thread.sleep(1_000);

            // 4. Close window
            WatermarkWindow closedWindow = watermarkWriter.closeWindow(window);

            // 5. Collect LOG events that arrived during the window
            List<ChangeEnvelope> logEventsDuringWindow = allWalEvents.stream()
                    .filter(e -> e.tableId().equals(TABLE_ID))
                    .filter(e -> e.captureMode() == CaptureMode.LOG)
                    .toList();

            // Run reconciliation
            var engine = new DefaultReconciliationEngine();
            ReconciliationResult result = engine.reconcile(closedWindow, logEventsDuringWindow, snapshotEvents);

            // The UPDATE for id=1 should be dropped (key collision with snapshot)
            assertThat(result.logEventsDropped())
                    .as("LOG event for id=1 should be dropped — snapshot data is newer")
                    .isGreaterThanOrEqualTo(1);

            // Snapshot event for id=1 should survive
            assertThat(result.mergedEvents().stream()
                    .filter(e -> e.captureMode() == CaptureMode.SNAPSHOT)
                    .anyMatch(e -> e.key().columns().get("id").equals(1)))
                    .as("Snapshot row for id=1 should survive reconciliation")
                    .isTrue();

            // Snapshot event for id=2 should also survive (no collision)
            assertThat(result.mergedEvents().stream()
                    .filter(e -> e.key().columns().get("id").equals(2))
                    .count())
                    .as("Row id=2 should appear exactly once (snapshot, no collision)")
                    .isEqualTo(1);
        }
    }

    // ── Test 5: WatermarkAwareEventRouter end-to-end ────────────────────

    @Test
    @Order(5)
    void watermarkAwareRouterCorrectlyMergesLiveAndSnapshotEvents() throws Exception {
        // This tests the full router: events pass through when no window,
        // buffer during window, reconcile on close

        var outputSink = new CopyOnWriteArrayList<ChangeEnvelope>();
        EventSink testSink = new EventSink() {
            @Override
            public PublishResult publish(ChangeEnvelope event) {
                outputSink.add(event);
                return new PublishResult.Success(1);
            }
            @Override
            public PublishResult publishBatch(List<ChangeEnvelope> events) {
                outputSink.addAll(events);
                return new PublishResult.Success(events.size());
            }
        };

        var router = new WatermarkAwareEventRouter(new DefaultReconciliationEngine(), testSink);

        // Events before window pass through
        router.onEvent(createLogEvent(100, OperationType.INSERT));
        assertThat(outputSink).hasSize(1);

        // Open window
        WatermarkWindow window = new WatermarkWindow(
                java.util.UUID.randomUUID(), TABLE_ID, Instant.now(), null, 0);
        router.openWindow(window);

        // LOG events during window are buffered
        router.onEvent(createLogEvent(1, OperationType.UPDATE)); // collides with snapshot
        router.onEvent(createLogEvent(3, OperationType.INSERT)); // no collision

        assertThat(outputSink).hasSize(1); // still just the pre-window event
        assertThat(router.bufferedLogEventCount()).isEqualTo(2);

        // Provide snapshot chunk data
        router.onSnapshotChunk(List.of(
                createSnapshotEvent(1, "Snapshot Product 1", 10.0),
                createSnapshotEvent(2, "Snapshot Product 2", 20.0)
        ));

        // Close window — triggers reconciliation
        router.closeWindow();

        // Output should now have: pre-window event + surviving log (id=3) + snapshots (id=1, id=2)
        assertThat(outputSink).hasSize(4);

        // Pre-window event (id=100)
        assertThat(outputSink.get(0).key().columns().get("id")).isEqualTo(100);

        // Surviving log event (id=3, no collision)
        assertThat(outputSink.get(1).captureMode()).isEqualTo(CaptureMode.LOG);
        assertThat(outputSink.get(1).key().columns().get("id")).isEqualTo(3);

        // Snapshot events (id=1 and id=2)
        assertThat(outputSink.get(2).captureMode()).isEqualTo(CaptureMode.SNAPSHOT);
        assertThat(outputSink.get(3).captureMode()).isEqualTo(CaptureMode.SNAPSHOT);

        // After close, events pass through again
        router.onEvent(createLogEvent(200, OperationType.INSERT));
        assertThat(outputSink).hasSize(5);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private static Connection openConnection() throws SQLException {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private void waitForEventsFromTable(TableId tableId, int minCount, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            long count = allWalEvents.stream()
                    .filter(e -> e.tableId().equals(tableId))
                    .count();
            if (count >= minCount) return;
            Thread.sleep(200);
        }
        long actual = allWalEvents.stream().filter(e -> e.tableId().equals(tableId)).count();
        assertThat(actual)
                .as("Expected at least %d events from %s within %s, got %d", minCount, tableId, timeout, actual)
                .isGreaterThanOrEqualTo(minCount);
    }

    private ChangeEnvelope createSnapshotEvent(int id, String name, double price) {
        return new ChangeEnvelope(
                java.util.UUID.randomUUID(), "test-connector", TABLE_ID,
                OperationType.SNAPSHOT_READ, CaptureMode.SNAPSHOT,
                new RowKey(java.util.Map.of("id", id)),
                null, java.util.Map.of("id", id, "name", name, "price", price),
                null, Instant.now(), null, java.util.Map.of()
        );
    }

    private ChangeEnvelope createLogEvent(int id, OperationType op) {
        return new ChangeEnvelope(
                java.util.UUID.randomUUID(), "test-connector", TABLE_ID,
                op, CaptureMode.LOG,
                new RowKey(java.util.Map.of("id", id)),
                null, java.util.Map.of("id", id, "name", "log-" + id),
                new SourcePosition(5000L + id, Instant.now()),
                Instant.now(), null, java.util.Map.of()
        );
    }
}
