package com.paulsnow.qcdcf.core.connector;

/**
 * Lifecycle states of a CDC connector.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public enum ConnectorStatus {

    /** Connector is initialising. */
    STARTING,

    /** Connector is streaming WAL events. */
    RUNNING,

    /** Connector is performing a snapshot. */
    SNAPSHOTTING,

    /** Connector has been gracefully stopped. */
    STOPPED,

    /** Connector has encountered an unrecoverable error. */
    FAILED
}
