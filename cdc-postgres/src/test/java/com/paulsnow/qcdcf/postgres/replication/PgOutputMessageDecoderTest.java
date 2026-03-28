package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.model.OperationType;
import com.paulsnow.qcdcf.model.TableId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PgOutputMessageDecoder}.
 * <p>
 * Each test constructs hand-crafted pgoutput binary messages and verifies
 * the decoder produces the correct structured output.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
class PgOutputMessageDecoderTest {

    private PgOutputMessageDecoder decoder;

    @BeforeEach
    void setUp() {
        decoder = new PgOutputMessageDecoder();
    }

    // ---- Begin message ----

    @Test
    void decodesBeginMessage() {
        long finalLsn = 0x0000_0001_0000_00A0L;
        long pgTimestamp = 750_000_000_000_000L; // some timestamp since PG epoch
        int xid = 42;

        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 8 + 4);
        buf.put((byte) 'B');
        buf.putLong(finalLsn);
        buf.putLong(pgTimestamp);
        buf.putInt(xid);
        buf.flip();

        RawReplicationMessage raw = new RawReplicationMessage(100L, Instant.now(), buf);
        DecodedReplicationMessage msg = decoder.decode(raw);

        assertThat(msg).isNotNull();
        assertThat(msg.type()).isEqualTo(DecodedReplicationMessage.MessageType.BEGIN);
        assertThat(msg.lsn()).isEqualTo(finalLsn);
        assertThat(msg.txId()).isEqualTo(42L);
        assertThat(msg.commitTimestamp()).isEqualTo(
                PgOutputMessageDecoder.pgTimestampToInstant(pgTimestamp));
        assertThat(msg.tableId()).isNull();
        assertThat(msg.operation()).isNull();
    }

    // ---- Commit message ----

    @Test
    void decodesCommitMessage() {
        long commitLsn = 0x0000_0001_0000_00B0L;
        long endLsn = 0x0000_0001_0000_00C0L;
        long pgTimestamp = 750_000_000_000_000L;

        ByteBuffer buf = ByteBuffer.allocate(1 + 1 + 8 + 8 + 8);
        buf.put((byte) 'C');
        buf.put((byte) 0);       // flags
        buf.putLong(commitLsn);
        buf.putLong(endLsn);
        buf.putLong(pgTimestamp);
        buf.flip();

        RawReplicationMessage raw = new RawReplicationMessage(100L, Instant.now(), buf);
        DecodedReplicationMessage msg = decoder.decode(raw);

        assertThat(msg).isNotNull();
        assertThat(msg.type()).isEqualTo(DecodedReplicationMessage.MessageType.COMMIT);
        assertThat(msg.lsn()).isEqualTo(endLsn);
        assertThat(msg.txId()).isNull();
        assertThat(msg.commitTimestamp()).isEqualTo(
                PgOutputMessageDecoder.pgTimestampToInstant(pgTimestamp));
    }

    // ---- Relation message ----

    @Test
    void decodesRelationMessageAndPopulatesCache() {
        ByteBuffer buf = buildRelationMessage(16384, "public", "customer", 'd',
                new TestColumn("id", true, 23),
                new TestColumn("name", false, 25));

        RawReplicationMessage raw = new RawReplicationMessage(200L, Instant.now(), buf);
        DecodedReplicationMessage msg = decoder.decode(raw);

        // Relation messages return null (metadata only)
        assertThat(msg).isNull();

        // Verify the cache was populated
        assertThat(decoder.relationCache()).containsKey(16384);
        RelationMetadata meta = decoder.relationCache().get(16384);
        assertThat(meta.tableId()).isEqualTo(new TableId("public", "customer"));
        assertThat(meta.replicaIdentity()).isEqualTo('d');
        assertThat(meta.columns()).hasSize(2);
        assertThat(meta.columns().get(0).name()).isEqualTo("id");
        assertThat(meta.columns().get(0).partOfKey()).isTrue();
        assertThat(meta.columns().get(0).typeOid()).isEqualTo(23);
        assertThat(meta.columns().get(1).name()).isEqualTo("name");
        assertThat(meta.columns().get(1).partOfKey()).isFalse();
        assertThat(meta.keyColumnNames()).containsExactly("id");
    }

    // ---- Insert message ----

    @Test
    void decodesInsertMessage() {
        // First, seed the relation cache
        seedRelation(16384, "public", "customer",
                new TestColumn("id", true, 23),
                new TestColumn("name", false, 25));

        ByteBuffer buf = buildInsertMessage(16384, "42", "Alice");

        RawReplicationMessage raw = new RawReplicationMessage(300L, Instant.now(), buf);
        DecodedReplicationMessage msg = decoder.decode(raw);

        assertThat(msg).isNotNull();
        assertThat(msg.type()).isEqualTo(DecodedReplicationMessage.MessageType.INSERT);
        assertThat(msg.operation()).isEqualTo(OperationType.INSERT);
        assertThat(msg.tableId()).isEqualTo(new TableId("public", "customer"));
        assertThat(msg.newValues()).containsEntry("id", "42").containsEntry("name", "Alice");
        assertThat(msg.oldValues()).isNull();
        assertThat(msg.keyColumns()).containsExactly("id");
    }

    // ---- Update message with old key columns ----

    @Test
    void decodesUpdateMessageWithOldKeyColumns() {
        seedRelation(16384, "public", "customer",
                new TestColumn("id", true, 23),
                new TestColumn("name", false, 25));

        ByteBuffer buf = buildUpdateMessageWithOldKey(16384,
                new String[]{"42", "Alice"},    // old key values
                new String[]{"42", "Bob"});     // new values

        RawReplicationMessage raw = new RawReplicationMessage(400L, Instant.now(), buf);
        DecodedReplicationMessage msg = decoder.decode(raw);

        assertThat(msg).isNotNull();
        assertThat(msg.type()).isEqualTo(DecodedReplicationMessage.MessageType.UPDATE);
        assertThat(msg.operation()).isEqualTo(OperationType.UPDATE);
        assertThat(msg.oldValues()).containsEntry("id", "42").containsEntry("name", "Alice");
        assertThat(msg.newValues()).containsEntry("id", "42").containsEntry("name", "Bob");
    }

    // ---- Update message without old tuple ----

    @Test
    void decodesUpdateMessageWithoutOldTuple() {
        seedRelation(16384, "public", "customer",
                new TestColumn("id", true, 23),
                new TestColumn("name", false, 25));

        ByteBuffer buf = buildUpdateMessageWithoutOldTuple(16384, "42", "Charlie");

        RawReplicationMessage raw = new RawReplicationMessage(400L, Instant.now(), buf);
        DecodedReplicationMessage msg = decoder.decode(raw);

        assertThat(msg).isNotNull();
        assertThat(msg.type()).isEqualTo(DecodedReplicationMessage.MessageType.UPDATE);
        assertThat(msg.oldValues()).isNull();
        assertThat(msg.newValues()).containsEntry("id", "42").containsEntry("name", "Charlie");
    }

    // ---- Delete message ----

    @Test
    void decodesDeleteMessage() {
        seedRelation(16384, "public", "customer",
                new TestColumn("id", true, 23),
                new TestColumn("name", false, 25));

        ByteBuffer buf = buildDeleteMessage(16384, "42", "Alice");

        RawReplicationMessage raw = new RawReplicationMessage(500L, Instant.now(), buf);
        DecodedReplicationMessage msg = decoder.decode(raw);

        assertThat(msg).isNotNull();
        assertThat(msg.type()).isEqualTo(DecodedReplicationMessage.MessageType.DELETE);
        assertThat(msg.operation()).isEqualTo(OperationType.DELETE);
        assertThat(msg.oldValues()).containsEntry("id", "42").containsEntry("name", "Alice");
        assertThat(msg.newValues()).isNull();
        assertThat(msg.keyColumns()).containsExactly("id");
    }

    // ---- NULL column handling ----

    @Test
    void handlesNullColumnValues() {
        seedRelation(16384, "public", "customer",
                new TestColumn("id", true, 23),
                new TestColumn("name", false, 25));

        // Build an Insert with id = "42" and name = NULL
        ByteBuffer tuple = buildTupleData(new TupleValue("42"), TupleValue.ofNull());
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + tuple.remaining());
        buf.put((byte) 'I');
        buf.putInt(16384);
        buf.put((byte) 'N');
        buf.put(tuple);
        buf.flip();

        RawReplicationMessage raw = new RawReplicationMessage(600L, Instant.now(), buf);
        DecodedReplicationMessage msg = decoder.decode(raw);

        assertThat(msg).isNotNull();
        assertThat(msg.newValues()).containsEntry("id", "42");
        assertThat(msg.newValues()).containsKey("name");
        assertThat(msg.newValues().get("name")).isNull();
    }

    // ---- Unchanged TOAST handling ----

    @Test
    void handlesUnchangedToastValues() {
        seedRelation(16384, "public", "customer",
                new TestColumn("id", true, 23),
                new TestColumn("bio", false, 25));

        // Build an Insert with id = "42" and bio = unchanged TOAST
        ByteBuffer tuple = buildTupleData(new TupleValue("42"), TupleValue.ofUnchangedToast());
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + tuple.remaining());
        buf.put((byte) 'I');
        buf.putInt(16384);
        buf.put((byte) 'N');
        buf.put(tuple);
        buf.flip();

        RawReplicationMessage raw = new RawReplicationMessage(700L, Instant.now(), buf);
        DecodedReplicationMessage msg = decoder.decode(raw);

        assertThat(msg).isNotNull();
        assertThat(msg.newValues()).containsEntry("id", "42");
        // TOAST unchanged values should be omitted
        assertThat(msg.newValues()).doesNotContainKey("bio");
    }

    // ---- Unknown message tag ----

    @Test
    void returnsNullForUnknownTag() {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) 'Z');
        buf.flip();

        RawReplicationMessage raw = new RawReplicationMessage(800L, Instant.now(), buf);
        assertThat(decoder.decode(raw)).isNull();
    }

    // ---- Timestamp conversion ----

    @Test
    void convertsPostgresTimestampCorrectly() {
        // PG epoch is 2000-01-01T00:00:00Z
        // 0 microseconds since PG epoch = 2000-01-01T00:00:00Z
        Instant result = PgOutputMessageDecoder.pgTimestampToInstant(0L);
        assertThat(result).isEqualTo(Instant.parse("2000-01-01T00:00:00Z"));

        // 1 day = 86400 * 1_000_000 microseconds = 86400000000
        Instant oneDayAfter = PgOutputMessageDecoder.pgTimestampToInstant(86_400_000_000L);
        assertThat(oneDayAfter).isEqualTo(Instant.parse("2000-01-02T00:00:00Z"));
    }

    // ================ Helper methods ================

    private record TestColumn(String name, boolean partOfKey, int typeOid) {}

    /**
     * Represents a single column value within a tuple.
     */
    private record TupleValue(char tag, byte[] data) {
        TupleValue(String textValue) {
            this('t', textValue.getBytes(StandardCharsets.UTF_8));
        }

        static TupleValue ofNull() {
            return new TupleValue('n', null);
        }

        static TupleValue ofUnchangedToast() {
            return new TupleValue('u', null);
        }

        int byteSize() {
            return switch (tag) {
                case 't' -> 1 + 4 + data.length;
                default -> 1;
            };
        }
    }

    /**
     * Seeds the decoder's relation cache by sending a Relation message.
     */
    private void seedRelation(int oid, String schema, String table, TestColumn... columns) {
        ByteBuffer buf = buildRelationMessage(oid, schema, table, 'd', columns);
        decoder.decode(new RawReplicationMessage(0L, Instant.now(), buf));
    }

    /**
     * Builds a complete Relation ('R') message.
     */
    private ByteBuffer buildRelationMessage(int oid, String schema, String table,
                                            char replicaIdentity, TestColumn... columns) {
        byte[] schemaBytes = schema.getBytes(StandardCharsets.UTF_8);
        byte[] tableBytes = table.getBytes(StandardCharsets.UTF_8);

        int size = 1 + 4 + (schemaBytes.length + 1) + (tableBytes.length + 1) + 1 + 2;
        for (TestColumn col : columns) {
            byte[] nameBytes = col.name().getBytes(StandardCharsets.UTF_8);
            size += 1 + (nameBytes.length + 1) + 4 + 4;
        }

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 'R');
        buf.putInt(oid);
        buf.put(schemaBytes);
        buf.put((byte) 0);
        buf.put(tableBytes);
        buf.put((byte) 0);
        buf.put((byte) replicaIdentity);
        buf.putShort((short) columns.length);

        for (TestColumn col : columns) {
            buf.put((byte) (col.partOfKey() ? 1 : 0));
            buf.put(col.name().getBytes(StandardCharsets.UTF_8));
            buf.put((byte) 0);
            buf.putInt(col.typeOid());
            buf.putInt(-1); // type modifier
        }

        buf.flip();
        return buf;
    }

    /**
     * Builds a TupleData section from the given column values.
     */
    private ByteBuffer buildTupleData(TupleValue... values) {
        int size = 2; // column count
        for (TupleValue v : values) {
            size += v.byteSize();
        }
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putShort((short) values.length);
        for (TupleValue v : values) {
            buf.put((byte) v.tag());
            if (v.tag() == 't') {
                buf.putInt(v.data().length);
                buf.put(v.data());
            }
        }
        buf.flip();
        return buf;
    }

    /**
     * Builds an Insert ('I') message with text column values.
     */
    private ByteBuffer buildInsertMessage(int relationId, String... values) {
        TupleValue[] tupleValues = new TupleValue[values.length];
        for (int i = 0; i < values.length; i++) {
            tupleValues[i] = new TupleValue(values[i]);
        }
        ByteBuffer tuple = buildTupleData(tupleValues);

        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + tuple.remaining());
        buf.put((byte) 'I');
        buf.putInt(relationId);
        buf.put((byte) 'N');
        buf.put(tuple);
        buf.flip();
        return buf;
    }

    /**
     * Builds an Update ('U') message with old key columns ('K') and new tuple.
     */
    private ByteBuffer buildUpdateMessageWithOldKey(int relationId,
                                                    String[] oldValues, String[] newValues) {
        TupleValue[] oldTuple = new TupleValue[oldValues.length];
        for (int i = 0; i < oldValues.length; i++) {
            oldTuple[i] = new TupleValue(oldValues[i]);
        }
        TupleValue[] newTuple = new TupleValue[newValues.length];
        for (int i = 0; i < newValues.length; i++) {
            newTuple[i] = new TupleValue(newValues[i]);
        }

        ByteBuffer oldData = buildTupleData(oldTuple);
        ByteBuffer newData = buildTupleData(newTuple);

        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + oldData.remaining() + 1 + newData.remaining());
        buf.put((byte) 'U');
        buf.putInt(relationId);
        buf.put((byte) 'K');
        buf.put(oldData);
        buf.put((byte) 'N');
        buf.put(newData);
        buf.flip();
        return buf;
    }

    /**
     * Builds an Update ('U') message without an old tuple (just 'N' + new tuple).
     */
    private ByteBuffer buildUpdateMessageWithoutOldTuple(int relationId, String... values) {
        TupleValue[] tupleValues = new TupleValue[values.length];
        for (int i = 0; i < values.length; i++) {
            tupleValues[i] = new TupleValue(values[i]);
        }
        ByteBuffer tuple = buildTupleData(tupleValues);

        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + tuple.remaining());
        buf.put((byte) 'U');
        buf.putInt(relationId);
        buf.put((byte) 'N');
        buf.put(tuple);
        buf.flip();
        return buf;
    }

    /**
     * Builds a Delete ('D') message with old key columns.
     */
    private ByteBuffer buildDeleteMessage(int relationId, String... values) {
        TupleValue[] tupleValues = new TupleValue[values.length];
        for (int i = 0; i < values.length; i++) {
            tupleValues[i] = new TupleValue(values[i]);
        }
        ByteBuffer tuple = buildTupleData(tupleValues);

        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + tuple.remaining());
        buf.put((byte) 'D');
        buf.putInt(relationId);
        buf.put((byte) 'K');
        buf.put(tuple);
        buf.flip();
        return buf;
    }
}
