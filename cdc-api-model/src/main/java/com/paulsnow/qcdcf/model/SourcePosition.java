package com.paulsnow.qcdcf.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents a position in the PostgreSQL write-ahead log.
 *
 * @param lsn             the log sequence number
 * @param txId            the transaction ID (may be null for non-transactional events)
 * @param commitTimestamp  the commit timestamp of the transaction
 * @author Paul Snow
 * @since 0.0.0
 */
public record SourcePosition(long lsn, Long txId, Instant commitTimestamp) implements Comparable<SourcePosition> {

    public SourcePosition {
        Objects.requireNonNull(commitTimestamp, "commitTimestamp must not be null");
    }

    /** Convenience constructor without transaction ID. */
    public SourcePosition(long lsn, Instant commitTimestamp) {
        this(lsn, null, commitTimestamp);
    }

    @Override
    public int compareTo(SourcePosition other) {
        return Long.compare(this.lsn, other.lsn);
    }

    @Override
    public String toString() {
        return "SourcePosition[lsn=" + lsn + ", txId=" + txId + ", ts=" + commitTimestamp + "]";
    }
}
