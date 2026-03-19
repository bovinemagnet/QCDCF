package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.model.OperationType;
import com.paulsnow.qcdcf.model.TableId;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * A decoded replication message with structured column data.
 * <p>
 * Represents all pgoutput message types: BEGIN, COMMIT, INSERT, UPDATE, and DELETE.
 * For transactional envelope messages (BEGIN/COMMIT), table-specific fields are null.
 *
 * @param type            the decoded message type
 * @param lsn             the log sequence number
 * @param txId            the transaction ID (null for COMMIT)
 * @param commitTimestamp  the commit timestamp
 * @param tableId         the affected table (null for BEGIN/COMMIT)
 * @param operation       the operation type (null for BEGIN/COMMIT)
 * @param oldValues       column values before the change (null for INSERT, BEGIN, COMMIT)
 * @param newValues       column values after the change (null for DELETE, BEGIN, COMMIT)
 * @param keyColumns      names of columns forming the replica identity key (null for BEGIN/COMMIT)
 * @author Paul Snow
 * @since 0.0.0
 */
public record DecodedReplicationMessage(
        MessageType type,
        long lsn,
        Long txId,
        Instant commitTimestamp,
        TableId tableId,
        OperationType operation,
        Map<String, Object> oldValues,
        Map<String, Object> newValues,
        List<String> keyColumns
) {

    /**
     * The type of pgoutput protocol message.
     */
    public enum MessageType {
        /** Transaction begin envelope. */
        BEGIN,
        /** Transaction commit envelope. */
        COMMIT,
        /** Row insert. */
        INSERT,
        /** Row update. */
        UPDATE,
        /** Row delete. */
        DELETE
    }
}
