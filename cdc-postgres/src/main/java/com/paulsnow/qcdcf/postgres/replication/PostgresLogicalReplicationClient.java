package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.core.exception.SourceReadException;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;

/**
 * Manages PostgreSQL logical replication connections and streams WAL changes.
 * <p>
 * This client opens a dedicated replication connection to PostgreSQL using the JDBC
 * driver's replication protocol support. It reads from a named logical replication
 * slot using the {@code pgoutput} output plugin and delivers raw messages to a
 * {@link RawReplicationMessageHandler}.
 * <p>
 * The client supports slot lifecycle management (creation and deletion) via regular
 * JDBC connections, and WAL streaming via a separate replication-mode connection.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PostgresLogicalReplicationClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresLogicalReplicationClient.class);

    /**
     * Brief pause in milliseconds when no message is available from the stream.
     */
    private static final long POLL_INTERVAL_MS = 10;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String slotName;
    private final String publicationName;

    private Connection replicationConnection;
    private PGReplicationStream stream;
    private volatile boolean running;

    /**
     * Creates a new replication client.
     *
     * @param jdbcUrl         the JDBC URL for the PostgreSQL database
     * @param username        the database username (must have replication privileges)
     * @param password        the database password
     * @param slotName        the name of the logical replication slot to use
     * @param publicationName the name of the PostgreSQL publication to subscribe to
     */
    public PostgresLogicalReplicationClient(String jdbcUrl,
                                            String username,
                                            String password,
                                            String slotName,
                                            String publicationName) {
        this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl must not be null");
        this.username = Objects.requireNonNull(username, "username must not be null");
        this.password = Objects.requireNonNull(password, "password must not be null");
        this.slotName = Objects.requireNonNull(slotName, "slotName must not be null");
        this.publicationName = Objects.requireNonNull(publicationName, "publicationName must not be null");
    }

    /**
     * Creates the replication slot if it does not already exist.
     * <p>
     * This method uses a <em>regular</em> (non-replication) connection to issue the
     * slot creation command. If the slot already exists (SQL state {@code 42710}),
     * the error is silently ignored.
     *
     * @param connection a regular JDBC connection to the database
     * @throws SourceReadException if slot creation fails for a reason other than duplicate
     */
    public void createSlotIfNotExists(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slotName));
            LOG.info("Created replication slot: {}", slotName);
        } catch (SQLException e) {
            if ("42710".equals(e.getSQLState())) {
                LOG.debug("Replication slot already exists: {}", slotName);
            } else {
                throw new SourceReadException("Failed to create replication slot: " + slotName, e);
            }
        }
    }

    /**
     * Drops the replication slot.
     *
     * @param connection a regular JDBC connection to the database
     * @throws SourceReadException if the drop operation fails
     */
    public void dropSlot(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "SELECT pg_drop_replication_slot('%s')", slotName));
            LOG.info("Dropped replication slot: {}", slotName);
        } catch (SQLException e) {
            throw new SourceReadException("Failed to drop replication slot: " + slotName, e);
        }
    }

    /**
     * Starts streaming WAL changes from the replication slot.
     * <p>
     * This method opens a replication-mode connection, begins logical streaming from the
     * given LSN, and enters a read loop that delivers each message to the provided handler.
     * The loop continues until {@link #stop()} is called.
     * <p>
     * This method <strong>blocks</strong> the calling thread for the duration of streaming.
     *
     * @param startLsn the LSN to begin streaming from (use {@code 0} to start from the slot's
     *                 confirmed position)
     * @param handler  the handler to receive raw replication messages
     * @throws SourceReadException if the replication connection or stream cannot be established,
     *                             or if an error occurs during streaming
     */
    public void start(long startLsn, RawReplicationMessageHandler handler) {
        Objects.requireNonNull(handler, "handler must not be null");

        running = true;
        LOG.info("Starting replication stream for slot '{}', publication '{}', from LSN {}",
                slotName, publicationName, LogSequenceNumber.valueOf(startLsn));

        try {
            openReplicationConnection();
            startStream(startLsn);
            readLoop(handler);
        } catch (SQLException e) {
            if (running) {
                throw new SourceReadException(
                        "Error during replication streaming on slot: " + slotName, e);
            }
            LOG.debug("SQLException during shutdown of slot '{}': {}", slotName, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Replication stream interrupted for slot '{}'", slotName);
        } finally {
            running = false;
            LOG.info("Replication stream ended for slot '{}'", slotName);
        }
    }

    /**
     * Acknowledges that all messages up to and including the given LSN have been processed.
     * <p>
     * This updates both the applied and flushed positions on the stream and forces a
     * status update to PostgreSQL, allowing the server to discard the acknowledged WAL segments.
     *
     * @param lsn the log sequence number to acknowledge
     * @throws SourceReadException if the acknowledgement fails
     */
    public void acknowledgeLsn(long lsn) {
        if (stream == null) {
            LOG.warn("Cannot acknowledge LSN — stream is not open");
            return;
        }
        try {
            LogSequenceNumber lsnValue = LogSequenceNumber.valueOf(lsn);
            stream.setAppliedLSN(lsnValue);
            stream.setFlushedLSN(lsnValue);
            stream.forceUpdateStatus();
            LOG.trace("Acknowledged LSN {}", lsnValue);
        } catch (SQLException e) {
            throw new SourceReadException("Failed to acknowledge LSN: " + lsn, e);
        }
    }

    /**
     * Signals the streaming loop to stop.
     * <p>
     * The streaming loop in {@link #start(long, RawReplicationMessageHandler)} will exit
     * on its next iteration after this method is called. This method is thread-safe.
     */
    public void stop() {
        running = false;
        LOG.info("Stop requested for replication slot '{}'", slotName);
    }

    /**
     * Stops streaming and closes the replication connection and stream.
     */
    @Override
    public void close() {
        stop();
        closeStream();
        closeConnection();
    }

    /**
     * Returns the name of the replication slot.
     *
     * @return the slot name
     */
    public String slotName() {
        return slotName;
    }

    /**
     * Returns the name of the publication.
     *
     * @return the publication name
     */
    public String publicationName() {
        return publicationName;
    }

    /**
     * Returns whether the client is currently streaming.
     *
     * @return {@code true} if the streaming loop is active
     */
    public boolean isRunning() {
        return running;
    }

    // ---- internal ----

    private void openReplicationConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("assumeMinServerVersion", "16");
        props.setProperty("replication", "database");

        replicationConnection = DriverManager.getConnection(jdbcUrl, props);
        LOG.debug("Opened replication connection to {}", jdbcUrl);
    }

    private void startStream(long startLsn) throws SQLException {
        PGConnection pgConn = replicationConnection.unwrap(PGConnection.class);

        stream = pgConn.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withStartPosition(LogSequenceNumber.valueOf(startLsn))
                .withSlotOption("proto_version", "1")
                .withSlotOption("publication_names", publicationName)
                .start();

        LOG.debug("Replication stream started on slot '{}' from LSN {}", slotName,
                LogSequenceNumber.valueOf(startLsn));
    }

    private void readLoop(RawReplicationMessageHandler handler) throws SQLException, InterruptedException {
        while (running) {
            ByteBuffer msg = stream.readPending();

            if (msg == null) {
                Thread.sleep(POLL_INTERVAL_MS);
                continue;
            }

            LogSequenceNumber lastLsn = stream.getLastReceiveLSN();
            long lsn = lastLsn.asLong();

            RawReplicationMessage rawMessage = new RawReplicationMessage(
                    lsn,
                    Instant.now(),
                    msg
            );

            LOG.trace("Received replication message at LSN {}, {} bytes",
                    lastLsn, msg.remaining());

            handler.handle(rawMessage);
        }
    }

    private void closeStream() {
        if (stream != null) {
            try {
                stream.close();
                LOG.debug("Closed replication stream for slot '{}'", slotName);
            } catch (SQLException e) {
                LOG.warn("Error closing replication stream for slot '{}': {}", slotName, e.getMessage());
            } finally {
                stream = null;
            }
        }
    }

    private void closeConnection() {
        if (replicationConnection != null) {
            try {
                replicationConnection.close();
                LOG.debug("Closed replication connection for slot '{}'", slotName);
            } catch (SQLException e) {
                LOG.warn("Error closing replication connection for slot '{}': {}", slotName, e.getMessage());
            } finally {
                replicationConnection = null;
            }
        }
    }
}
