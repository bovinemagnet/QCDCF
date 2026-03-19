package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.model.OperationType;
import com.paulsnow.qcdcf.model.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Decodes pgoutput protocol messages into structured {@link DecodedReplicationMessage} objects.
 * <p>
 * The pgoutput plugin is the built-in logical decoding output plugin in PostgreSQL 10+.
 * It produces a binary protocol with message types: Begin, Relation, Insert, Update, Delete, Commit.
 * <p>
 * This decoder maintains an internal cache of relation metadata received via 'R' messages,
 * which is used to resolve column names when decoding data-change messages.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PgOutputMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(PgOutputMessageDecoder.class);

    /**
     * Microsecond offset between the PostgreSQL epoch (2000-01-01) and the Unix epoch (1970-01-01).
     * PostgreSQL timestamps are microseconds since 2000-01-01 00:00:00 UTC.
     */
    static final long PG_EPOCH_OFFSET_MICROS = 946_684_800_000_000L;

    private final Map<Integer, RelationMetadata> relationCache = new ConcurrentHashMap<>();

    /**
     * Decodes a raw replication message into a structured form.
     *
     * @param message the raw message to decode
     * @return the decoded message, or {@code null} if the message type is not recognised
     */
    public DecodedReplicationMessage decode(RawReplicationMessage message) {
        ByteBuffer buf = message.data().duplicate();
        if (!buf.hasRemaining()) {
            LOG.warn("Empty replication message at LSN {}", message.lsn());
            return null;
        }

        char tag = (char) buf.get();
        LOG.debug("Decoding pgoutput message tag '{}' at LSN {}", tag, message.lsn());

        return switch (tag) {
            case 'B' -> decodeBegin(buf, message.lsn());
            case 'C' -> decodeCommit(buf, message.lsn());
            case 'R' -> decodeRelation(buf, message.lsn());
            case 'I' -> decodeInsert(buf, message.lsn());
            case 'U' -> decodeUpdate(buf, message.lsn());
            case 'D' -> decodeDelete(buf, message.lsn());
            default -> {
                LOG.debug("Ignoring unrecognised pgoutput message tag '{}' at LSN {}", tag, message.lsn());
                yield null;
            }
        };
    }

    /**
     * Returns the current relation metadata cache (primarily for testing).
     *
     * @return unmodifiable view of the relation cache
     */
    public Map<Integer, RelationMetadata> relationCache() {
        return Map.copyOf(relationCache);
    }

    // ---- Begin (tag 'B') ----

    private DecodedReplicationMessage decodeBegin(ByteBuffer buf, long lsn) {
        long finalLsn = buf.getLong();
        long pgTimestamp = buf.getLong();
        int xid = buf.getInt();

        return new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.BEGIN,
                finalLsn,
                (long) xid,
                pgTimestampToInstant(pgTimestamp),
                null, null, null, null, null
        );
    }

    // ---- Commit (tag 'C') ----

    private DecodedReplicationMessage decodeCommit(ByteBuffer buf, long lsn) {
        buf.get(); // flags (currently unused)
        long commitLsn = buf.getLong();
        long endLsn = buf.getLong();
        long pgTimestamp = buf.getLong();

        return new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.COMMIT,
                endLsn,
                null,
                pgTimestampToInstant(pgTimestamp),
                null, null, null, null, null
        );
    }

    // ---- Relation (tag 'R') ----

    private DecodedReplicationMessage decodeRelation(ByteBuffer buf, long lsn) {
        int relationId = buf.getInt();
        String namespace = readNullTerminatedString(buf);
        String relationName = readNullTerminatedString(buf);
        char replicaIdentity = (char) buf.get();
        int columnCount = buf.getShort() & 0xFFFF;

        List<RelationMetadata.ColumnInfo> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            int flags = buf.get() & 0xFF;
            boolean partOfKey = (flags & 1) != 0;
            String columnName = readNullTerminatedString(buf);
            int typeOid = buf.getInt();
            int typeMod = buf.getInt(); // type modifier — not stored but must be consumed
            columns.add(new RelationMetadata.ColumnInfo(columnName, partOfKey, typeOid));
        }

        TableId tableId = new TableId(namespace, relationName);
        RelationMetadata metadata = new RelationMetadata(relationId, tableId, replicaIdentity, List.copyOf(columns));
        relationCache.put(relationId, metadata);

        LOG.debug("Cached relation metadata for {} (OID {}): {} columns", tableId, relationId, columnCount);
        return null; // Relation messages are metadata-only; not forwarded as data changes
    }

    // ---- Insert (tag 'I') ----

    private DecodedReplicationMessage decodeInsert(ByteBuffer buf, long lsn) {
        int relationId = buf.getInt();
        RelationMetadata meta = lookupRelation(relationId);
        if (meta == null) {
            return null;
        }

        char tupleTag = (char) buf.get(); // should be 'N'
        if (tupleTag != 'N') {
            LOG.warn("Expected 'N' tuple tag in Insert message, got '{}'", tupleTag);
            return null;
        }

        Map<String, Object> newValues = decodeTupleData(buf, meta);

        return new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.INSERT,
                lsn,
                null,
                null,
                meta.tableId(),
                OperationType.INSERT,
                null,
                newValues,
                meta.keyColumnNames()
        );
    }

    // ---- Update (tag 'U') ----

    private DecodedReplicationMessage decodeUpdate(ByteBuffer buf, long lsn) {
        int relationId = buf.getInt();
        RelationMetadata meta = lookupRelation(relationId);
        if (meta == null) {
            return null;
        }

        Map<String, Object> oldValues = null;
        char marker = (char) buf.get();

        // Optional old tuple: 'K' (key only) or 'O' (full old tuple)
        if (marker == 'K' || marker == 'O') {
            oldValues = decodeTupleData(buf, meta);
            marker = (char) buf.get(); // advance to the 'N' tag
        }

        if (marker != 'N') {
            LOG.warn("Expected 'N' tuple tag in Update message, got '{}'", marker);
            return null;
        }

        Map<String, Object> newValues = decodeTupleData(buf, meta);

        return new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.UPDATE,
                lsn,
                null,
                null,
                meta.tableId(),
                OperationType.UPDATE,
                oldValues,
                newValues,
                meta.keyColumnNames()
        );
    }

    // ---- Delete (tag 'D') ----

    private DecodedReplicationMessage decodeDelete(ByteBuffer buf, long lsn) {
        int relationId = buf.getInt();
        RelationMetadata meta = lookupRelation(relationId);
        if (meta == null) {
            return null;
        }

        char tupleTag = (char) buf.get(); // 'K' or 'O'
        if (tupleTag != 'K' && tupleTag != 'O') {
            LOG.warn("Expected 'K' or 'O' tuple tag in Delete message, got '{}'", tupleTag);
            return null;
        }

        Map<String, Object> oldValues = decodeTupleData(buf, meta);

        return new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.DELETE,
                lsn,
                null,
                null,
                meta.tableId(),
                OperationType.DELETE,
                oldValues,
                null,
                meta.keyColumnNames()
        );
    }

    // ---- Tuple data decoding ----

    private Map<String, Object> decodeTupleData(ByteBuffer buf, RelationMetadata meta) {
        int columnCount = buf.getShort() & 0xFFFF;
        Map<String, Object> values = new LinkedHashMap<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            String columnName = (i < meta.columns().size())
                    ? meta.columns().get(i).name()
                    : "unknown_" + i;

            char colTag = (char) buf.get();
            switch (colTag) {
                case 'n' -> values.put(columnName, null);                   // NULL
                case 'u' -> { /* unchanged TOAST — omit from map */ }
                case 't' -> {
                    int length = buf.getInt();
                    byte[] bytes = new byte[length];
                    buf.get(bytes);
                    values.put(columnName, new String(bytes, StandardCharsets.UTF_8));
                }
                default -> LOG.warn("Unknown column data tag '{}' for column '{}'", colTag, columnName);
            }
        }

        return java.util.Collections.unmodifiableMap(values);
    }

    // ---- Helper methods ----

    private RelationMetadata lookupRelation(int relationId) {
        RelationMetadata meta = relationCache.get(relationId);
        if (meta == null) {
            LOG.warn("No cached relation metadata for OID {}. Was a Relation message missed?", relationId);
        }
        return meta;
    }

    /**
     * Reads a null-terminated UTF-8 string from the buffer.
     */
    static String readNullTerminatedString(ByteBuffer buf) {
        int start = buf.position();
        while (buf.hasRemaining() && buf.get() != 0) {
            // advance past the null terminator
        }
        int end = buf.position() - 1; // exclude the null byte
        byte[] bytes = new byte[end - start];
        buf.position(start);
        buf.get(bytes);
        buf.get(); // consume the null terminator
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Converts a PostgreSQL timestamp (microseconds since 2000-01-01) to a Java {@link Instant}.
     */
    static Instant pgTimestampToInstant(long pgMicros) {
        long unixMicros = pgMicros + PG_EPOCH_OFFSET_MICROS;
        long seconds = Math.floorDiv(unixMicros, 1_000_000L);
        long nanoAdjustment = Math.floorMod(unixMicros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanoAdjustment);
    }
}
