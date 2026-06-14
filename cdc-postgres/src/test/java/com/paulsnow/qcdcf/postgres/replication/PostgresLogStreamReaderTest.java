package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.InMemoryEventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostgresLogStreamReader} LSN acknowledgement semantics.
 * <p>
 * Verifies the at-least-once guarantee: a transaction containing a failed
 * publication must not be acknowledged to PostgreSQL, otherwise the WAL for
 * those events is discarded and the data is lost.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
class PostgresLogStreamReaderTest {

    private static final int RELATION_OID = 16384;

    private RecordingClient client;
    private PgOutputMessageDecoder decoder;

    @BeforeEach
    void setUp() {
        client = new RecordingClient();
        decoder = new PgOutputMessageDecoder();
    }

    @Test
    void successfulTransactionIsAcknowledged() {
        var sink = new InMemoryEventSink();
        var reader = newReader(sink);

        feedTransaction(reader, 500L);

        assertThat(sink.events()).hasSize(1);
        assertThat(client.acknowledgedLsns).containsExactly(500L);
        assertThat(reader.lastProcessedLsn()).isEqualTo(500L);
    }

    @Test
    void transactionWithFailedPublicationIsNotAcknowledged() {
        var sink = new FailingSink();
        var reader = newReader(sink);

        feedTransaction(reader, 500L);

        assertThat(client.acknowledgedLsns).isEmpty();
        assertThat(reader.lastProcessedLsn()).isNotEqualTo(500L);
    }

    @Test
    void failureInOneTransactionDoesNotBlockTheNext() {
        var sink = new FailingSink();
        var reader = newReader(sink);

        feedTransaction(reader, 500L);
        assertThat(client.acknowledgedLsns).isEmpty();

        sink.failing = false;
        feedTransaction(reader, 600L);

        assertThat(client.acknowledgedLsns).containsExactly(600L);
        assertThat(reader.lastProcessedLsn()).isEqualTo(600L);
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private PostgresLogStreamReader newReader(EventSink sink) {
        var reader = new PostgresLogStreamReader(
                client, decoder, new PgOutputEventNormaliser("test"), sink);
        reader.handleRawMessage(new RawReplicationMessage(0L, Instant.now(), relationMessage()));
        return reader;
    }

    /** Feeds BEGIN → INSERT → COMMIT, committing at the given LSN. */
    private void feedTransaction(PostgresLogStreamReader reader, long commitLsn) {
        reader.handleRawMessage(new RawReplicationMessage(commitLsn - 100,
                Instant.now(), beginMessage(commitLsn)));
        reader.handleRawMessage(new RawReplicationMessage(commitLsn - 50,
                Instant.now(), insertMessage("42", "Alice")));
        reader.handleRawMessage(new RawReplicationMessage(commitLsn,
                Instant.now(), commitMessage(commitLsn)));
    }

    private ByteBuffer beginMessage(long finalLsn) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 8 + 4);
        buf.put((byte) 'B');
        buf.putLong(finalLsn);
        buf.putLong(750_000_000_000_000L);
        buf.putInt(42);
        buf.flip();
        return buf;
    }

    private ByteBuffer commitMessage(long endLsn) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 1 + 8 + 8 + 8);
        buf.put((byte) 'C');
        buf.put((byte) 0);
        buf.putLong(endLsn - 10);
        buf.putLong(endLsn);
        buf.putLong(750_000_000_000_000L);
        buf.flip();
        return buf;
    }

    private ByteBuffer relationMessage() {
        byte[] schema = "public".getBytes(StandardCharsets.UTF_8);
        byte[] table = "customer".getBytes(StandardCharsets.UTF_8);
        byte[] col1 = "id".getBytes(StandardCharsets.UTF_8);
        byte[] col2 = "name".getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + schema.length + 1 + table.length + 1 + 1 + 2
                + (1 + col1.length + 1 + 4 + 4) + (1 + col2.length + 1 + 4 + 4));
        buf.put((byte) 'R');
        buf.putInt(RELATION_OID);
        buf.put(schema).put((byte) 0);
        buf.put(table).put((byte) 0);
        buf.put((byte) 'd');
        buf.putShort((short) 2);
        buf.put((byte) 1).put(col1).put((byte) 0).putInt(23).putInt(-1);
        buf.put((byte) 0).put(col2).put((byte) 0).putInt(25).putInt(-1);
        buf.flip();
        return buf;
    }

    private ByteBuffer insertMessage(String... values) {
        int tupleSize = 2;
        for (String v : values) {
            tupleSize += 1 + 4 + v.getBytes(StandardCharsets.UTF_8).length;
        }
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + tupleSize);
        buf.put((byte) 'I');
        buf.putInt(RELATION_OID);
        buf.put((byte) 'N');
        buf.putShort((short) values.length);
        for (String v : values) {
            byte[] data = v.getBytes(StandardCharsets.UTF_8);
            buf.put((byte) 't');
            buf.putInt(data.length);
            buf.put(data);
        }
        buf.flip();
        return buf;
    }

    /** Replication client that records acknowledged LSNs instead of talking to PostgreSQL. */
    private static class RecordingClient extends PostgresLogicalReplicationClient {
        final List<Long> acknowledgedLsns = new ArrayList<>();

        RecordingClient() {
            super("jdbc:postgresql://localhost/test", "user", "pass", "slot", "pub");
        }

        @Override
        public void acknowledgeLsn(long lsn) {
            acknowledgedLsns.add(lsn);
        }
    }

    /** Sink whose publish fails until {@code failing} is set to false. */
    private static class FailingSink extends InMemoryEventSink {
        boolean failing = true;

        @Override
        public PublishResult publish(ChangeEnvelope event) {
            if (failing) {
                return new PublishResult.Failure("simulated sink failure");
            }
            return super.publish(event);
        }
    }
}
