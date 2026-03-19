package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.CaptureMode;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.model.OperationType;
import com.paulsnow.qcdcf.testkit.postgres.PostgresContainerSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for WAL capture via the full CDC pipeline.
 * <p>
 * Exercises the path: PostgreSQL WAL → {@link PostgresLogicalReplicationClient}
 * → {@link PgOutputMessageDecoder} → {@link PgOutputEventNormaliser} → {@link WalCaptureIT.ThreadSafeEventSink}.
 * <p>
 * A single Testcontainers PostgreSQL container is shared across all tests to avoid
 * repeated container start/stop overhead. The replication reader runs on a daemon thread
 * for the lifetime of the test class.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Testcontainers
@org.junit.jupiter.api.TestMethodOrder(org.junit.jupiter.api.MethodOrderer.OrderAnnotation.class)
class WalCaptureIT {

    private static final String SCHEMA = "test_wal";
    private static final String TABLE = "customer";
    private static final String PUBLICATION = "test_wal_pub";
    private static final String SLOT = "test_wal_slot";
    private static final String CONNECTOR_ID = "test-connector";

    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(10);

    @Container
    static final PostgreSQLContainer<?> postgres = PostgresContainerSupport.createContainer();

    // Shared pipeline components — wired once in @BeforeAll.
    private static ThreadSafeEventSink sink;
    private static PostgresLogicalReplicationClient client;
    private static PostgresLogStreamReader reader;
    private static Thread readerThread;

    // -------------------------------------------------------------------------
    // Lifecycle — start the pipeline once for the whole test class.
    // -------------------------------------------------------------------------

    @BeforeAll
    static void startPipeline() throws Exception {
        try (Connection conn = openConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS test_wal.customer (
                        id    SERIAL PRIMARY KEY,
                        name  VARCHAR(255),
                        email VARCHAR(255)
                    )
                    """);

            // PostgreSQL 16 does not support IF NOT EXISTS on CREATE PUBLICATION.
            // The @BeforeAll runs once per class so there is no duplication risk.
            stmt.execute("CREATE PUBLICATION " + PUBLICATION
                    + " FOR TABLE " + SCHEMA + "." + TABLE);
        }

        // Wire the pipeline.
        client = new PostgresLogicalReplicationClient(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword(),
                SLOT,
                PUBLICATION);

        try (Connection conn = openConnection()) {
            client.createSlotIfNotExists(conn);
        }

        PgOutputMessageDecoder decoder = new PgOutputMessageDecoder();
        PgOutputEventNormaliser normaliser = new PgOutputEventNormaliser(CONNECTOR_ID);
        sink = new ThreadSafeEventSink();
        reader = new PostgresLogStreamReader(client, decoder, normaliser, sink);

        // The reader blocks its thread — run it as a daemon so the JVM can exit cleanly.
        // Capture any startup exceptions so test failures have clear diagnostics.
        var startupError = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        readerThread = new Thread(() -> {
            try {
                reader.start(0L);
            } catch (Throwable t) {
                startupError.set(t);
                System.err.println("WAL reader thread failed: " + t.getMessage());
                t.printStackTrace(System.err);
            }
        }, "wal-capture-it-reader");
        readerThread.setDaemon(true);
        readerThread.start();

        // Allow the replication stream time to establish before any test runs.
        Thread.sleep(2_000);

        // Fail fast if the reader thread died during startup.
        if (startupError.get() != null) {
            throw new RuntimeException("WAL reader failed to start", startupError.get());
        }
    }

    @AfterAll
    static void stopPipeline() throws Exception {
        if (reader != null) {
            reader.stop();
        }
        if (readerThread != null) {
            readerThread.join(5_000);
        }
        if (client != null) {
            client.close();
            // Drop the slot so subsequent runs (e.g. re-runs without container restart)
            // start from a clean state.
            try (Connection conn = openConnection()) {
                client.dropSlot(conn);
            } catch (Exception ignored) {
                // Slot may already be gone if the container restarted.
            }
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * An INSERT produces a single event with operation INSERT, captureMode LOG,
     * and the inserted column values present in the 'after' map.
     */
    @Test
    @org.junit.jupiter.api.Order(1)
    void insertProducesInsertEvent() throws Exception {
        drainAndClear();

        int id = insertCustomer("Alice", "alice@example.com");
        waitForEvents(sink, 1, EVENT_TIMEOUT);

        List<ChangeEnvelope> events = sink.events();
        assertThat(events).hasSize(1);

        ChangeEnvelope event = events.get(0);
        assertThat(event.operation())
                .as("operation type for INSERT")
                .isEqualTo(OperationType.INSERT);
        assertThat(event.captureMode())
                .as("capture mode must be LOG for WAL events")
                .isEqualTo(CaptureMode.LOG);

        // The 'after' map contains all column values after the insert.
        assertThat(event.after())
                .as("after map must contain the inserted name")
                .containsEntry("name", "Alice")
                .containsEntry("email", "alice@example.com");

        // The row key contains the primary-key column.
        assertThat(event.key().columns())
                .as("row key must contain the serial id")
                .containsKey("id");

        assertThat(event.tableId().schema()).isEqualTo(SCHEMA);
        assertThat(event.tableId().table()).isEqualTo(TABLE);
    }

    /**
     * An UPDATE produces an event with operation UPDATE and the new values in the 'after' map.
     */
    @Test
    @org.junit.jupiter.api.Order(2)
    void updateProducesUpdateEvent() throws Exception {
        drainAndClear();
        int id = insertCustomer("Bob", "bob@example.com");
        waitForEvents(sink, 1, EVENT_TIMEOUT);
        drainAndClear();

        executeUpdate("UPDATE test_wal.customer SET name = 'Robert' WHERE id = " + id);
        waitForEvents(sink, 1, EVENT_TIMEOUT);

        List<ChangeEnvelope> events = sink.events();
        assertThat(events).hasSize(1);

        ChangeEnvelope event = events.get(0);
        assertThat(event.operation())
                .as("operation type for UPDATE")
                .isEqualTo(OperationType.UPDATE);
        assertThat(event.after())
                .as("after map must reflect the new name")
                .containsEntry("name", "Robert");
    }

    /**
     * A DELETE produces an event with operation DELETE.
     * For a table with DEFAULT replica identity, pgoutput sends only key columns
     * in the old-tuple, so the 'before' map contains at minimum the primary key.
     */
    @Test
    @org.junit.jupiter.api.Order(3)
    void deleteProducesDeleteEvent() throws Exception {
        drainAndClear();
        int id = insertCustomer("Carol", "carol@example.com");
        waitForEvents(sink, 1, EVENT_TIMEOUT);
        drainAndClear();

        executeUpdate("DELETE FROM test_wal.customer WHERE id = " + id);
        waitForEvents(sink, 1, EVENT_TIMEOUT);

        List<ChangeEnvelope> events = sink.events();
        assertThat(events).hasSize(1);

        ChangeEnvelope event = events.get(0);
        assertThat(event.operation())
                .as("operation type for DELETE")
                .isEqualTo(OperationType.DELETE);
        assertThat(event.after())
                .as("after must be null for a DELETE")
                .isNull();

        // With DEFAULT replica identity, pgoutput delivers the key-only old tuple.
        // The row key is always populated via the key columns.
        assertThat(event.key().columns())
                .as("row key contains the deleted row's primary key")
                .containsKey("id");
    }

    /**
     * Events from two successive transactions arrive in commit order:
     * INSERT(row1), INSERT(row2), then UPDATE(row1).
     */
    @Test
    @org.junit.jupiter.api.Order(4)
    void eventsArriveInCommitOrder() throws Exception {
        drainAndClear();

        // Transaction 1: insert two rows.
        int idRow1;
        int idRow2;
        try (Connection conn = openConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO test_wal.customer (name, email) VALUES ('Dave', 'dave@example.com')");
                stmt.execute("INSERT INTO test_wal.customer (name, email) VALUES ('Eve', 'eve@example.com')");
            }
            conn.commit();

            // Retrieve the generated ids so we can correlate events.
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT id FROM test_wal.customer WHERE name IN ('Dave','Eve') ORDER BY id")) {
                rs.next();
                idRow1 = rs.getInt(1);
                rs.next();
                idRow2 = rs.getInt(1);
            }
        }

        // Wait for both INSERTs before issuing the UPDATE.
        waitForEvents(sink, 2, EVENT_TIMEOUT);

        // Transaction 2: update the first row.
        executeUpdate("UPDATE test_wal.customer SET name = 'David' WHERE id = " + idRow1);

        waitForEvents(sink, 3, EVENT_TIMEOUT);

        List<ChangeEnvelope> events = sink.events();
        assertThat(events).hasSizeGreaterThanOrEqualTo(3);

        // Events must arrive in commit order.
        assertThat(events.get(0).operation())
                .as("first event should be INSERT for row1")
                .isEqualTo(OperationType.INSERT);
        assertThat(events.get(0).after())
                .as("first INSERT is for Dave")
                .containsEntry("name", "Dave");

        assertThat(events.get(1).operation())
                .as("second event should be INSERT for row2")
                .isEqualTo(OperationType.INSERT);
        assertThat(events.get(1).after())
                .as("second INSERT is for Eve")
                .containsEntry("name", "Eve");

        assertThat(events.get(2).operation())
                .as("third event should be UPDATE for row1")
                .isEqualTo(OperationType.UPDATE);
        assertThat(events.get(2).after())
                .as("UPDATE changes name to David")
                .containsEntry("name", "David");
    }

    /**
     * Multiple DML operations on the same row within a single transaction
     * each produce a separate event.
     */
    @Test
    @org.junit.jupiter.api.Order(5)
    void multipleOperationsInSingleTransaction_produceSeparateEvents() throws Exception {
        drainAndClear();

        int id;
        try (Connection conn = openConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO test_wal.customer (name, email) VALUES ('Frank', 'frank@example.com')");
            }
            // Retrieve the newly inserted id within the same connection before committing.
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT currval('test_wal.customer_id_seq')")) {
                rs.next();
                id = rs.getInt(1);
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("UPDATE test_wal.customer SET name = 'Francis' WHERE id = " + id);
            }
            conn.commit();
        }

        // Both INSERT and UPDATE must arrive as separate events even though they are
        // from the same transaction.
        waitForEvents(sink, 2, EVENT_TIMEOUT);

        List<ChangeEnvelope> events = sink.events();
        assertThat(events).hasSizeGreaterThanOrEqualTo(2);

        ChangeEnvelope insertEvent = events.get(0);
        assertThat(insertEvent.operation())
                .as("first event in transaction is INSERT")
                .isEqualTo(OperationType.INSERT);
        assertThat(insertEvent.after())
                .as("INSERT captures original name Frank")
                .containsEntry("name", "Frank");

        ChangeEnvelope updateEvent = events.get(1);
        assertThat(updateEvent.operation())
                .as("second event in transaction is UPDATE")
                .isEqualTo(OperationType.UPDATE);
        assertThat(updateEvent.after())
                .as("UPDATE captures new name Francis")
                .containsEntry("name", "Francis");

        // Both events must share the same transaction id (set by PostgresLogStreamReader
        // via the BEGIN message enrichment).
        assertThat(insertEvent.position().txId())
                .as("INSERT and UPDATE share the same transaction id")
                .isEqualTo(updateEvent.position().txId());

        // Both events must refer to the same row key.
        assertThat(insertEvent.key().columns().get("id"))
                .as("both events target the same row")
                .isEqualTo(updateEvent.key().columns().get("id"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Drains any in-flight events from the sink, waits briefly, then clears.
     * This ensures events from a previous test don't leak into the next one.
     */
    private static void drainAndClear() throws InterruptedException {
        Thread.sleep(500);
        sink.clear();
    }

    /**
     * Opens a standard (non-replication) JDBC connection to the container.
     */
    private static Connection openConnection() throws SQLException {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword());
    }

    /**
     * Inserts a customer row and returns the generated serial id.
     */
    private static int insertCustomer(String name, String email) throws SQLException {
        try (Connection conn = openConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "INSERT INTO test_wal.customer (name, email) VALUES ('" + name + "', '" + email + "')");
            try (ResultSet rs = stmt.executeQuery("SELECT lastval()")) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    /**
     * Executes a DML statement (UPDATE or DELETE) on a regular connection.
     */
    private static void executeUpdate(String sql) throws SQLException {
        try (Connection conn = openConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /**
     * Waits until the sink holds at least {@code expectedCount} events, or until
     * {@code timeout} elapses. Fails the test if the count is not reached in time.
     * <p>
     * Uses a simple polling loop with 100 ms granularity rather than a third-party
     * await library to avoid additional dependencies.
     *
     * @param sink          the event sink to poll
     * @param expectedCount the minimum number of events to wait for
     * @param timeout       the maximum time to wait
     * @throws InterruptedException if the waiting thread is interrupted
     */
    private static void waitForEvents(ThreadSafeEventSink sink, int expectedCount, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (sink.events().size() < expectedCount && Instant.now().isBefore(deadline)) {
            Thread.sleep(100);
        }
        assertThat(sink.events())
                .as("expected at least %d events within %s, but got %d",
                        expectedCount, timeout, sink.events().size())
                .hasSizeGreaterThanOrEqualTo(expectedCount);
    }

    /**
     * A thread-safe {@link EventSink} backed by a {@link CopyOnWriteArrayList}.
     * <p>
     * {@link com.paulsnow.qcdcf.core.sink.InMemoryEventSink} uses a plain {@link java.util.ArrayList}
     * which is not safe for concurrent access between the background reader thread (calling
     * {@link #publish}) and the test thread (calling {@link #events}).  This local implementation
     * uses a {@link CopyOnWriteArrayList} so reads and clears are always consistent.
     */
    static final class ThreadSafeEventSink implements EventSink {

        private final CopyOnWriteArrayList<ChangeEnvelope> store = new CopyOnWriteArrayList<>();

        @Override
        public PublishResult publish(ChangeEnvelope event) {
            store.add(event);
            return new PublishResult.Success(1);
        }

        @Override
        public PublishResult publishBatch(List<ChangeEnvelope> batch) {
            store.addAll(batch);
            return new PublishResult.Success(batch.size());
        }

        /** Returns an unmodifiable snapshot of all captured events. */
        public List<ChangeEnvelope> events() {
            return Collections.unmodifiableList(store);
        }

        /** Removes all captured events. */
        public void clear() {
            store.clear();
        }
    }
}
