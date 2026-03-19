package com.paulsnow.qcdcf.model;

/**
 * Types of change operations captured by the CDC framework.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public enum OperationType {

    /** A new row was inserted. */
    INSERT,

    /** An existing row was updated. */
    UPDATE,

    /** An existing row was deleted. */
    DELETE,

    /** A row was read during a snapshot chunk. */
    SNAPSHOT_READ,

    /** A heartbeat event used for liveness tracking. */
    HEARTBEAT
}
