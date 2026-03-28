package com.paulsnow.qcdcf.model;

/**
 * Indicates how an event was captured.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public enum CaptureMode {

    /** Captured from the PostgreSQL write-ahead log. */
    LOG,

    /** Captured during a snapshot chunk read. */
    SNAPSHOT,

    /** Produced by the reconciliation engine after merging log and snapshot data. */
    RECONCILED
}
