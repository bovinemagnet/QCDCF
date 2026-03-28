package com.paulsnow.qcdcf.postgres.replication;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * A raw message received from the PostgreSQL logical replication stream.
 *
 * @param lsn       the log sequence number
 * @param timestamp the server timestamp
 * @param data      the raw message bytes
 * @author Paul Snow
 * @since 0.0.0
 */
public record RawReplicationMessage(long lsn, Instant timestamp, ByteBuffer data) {}
