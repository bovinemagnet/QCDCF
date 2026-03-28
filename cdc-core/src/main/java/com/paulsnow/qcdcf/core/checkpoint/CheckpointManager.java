package com.paulsnow.qcdcf.core.checkpoint;

import java.util.Optional;

/**
 * Persists and retrieves connector checkpoints for restart safety.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public interface CheckpointManager {

    /**
     * Saves a checkpoint. Replaces any existing checkpoint for the same connector.
     *
     * @param checkpoint the checkpoint to persist
     */
    void save(ConnectorCheckpoint checkpoint);

    /**
     * Loads the most recent checkpoint for the given connector.
     *
     * @param connectorId the connector identifier
     * @return the checkpoint, or empty if none exists
     */
    Optional<ConnectorCheckpoint> load(String connectorId);
}
