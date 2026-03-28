package com.paulsnow.qcdcf.postgres.replication;

/**
 * Callback interface for handling raw replication messages.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@FunctionalInterface
public interface RawReplicationMessageHandler {

    /**
     * Handles a raw replication message.
     *
     * @param message the raw message from the replication stream
     */
    void handle(RawReplicationMessage message);
}
