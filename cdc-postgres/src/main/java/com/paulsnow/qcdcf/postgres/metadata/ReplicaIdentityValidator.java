package com.paulsnow.qcdcf.postgres.metadata;

import com.paulsnow.qcdcf.core.exception.InvalidTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates that a table's configuration is suitable for CDC capture.
 * <p>
 * Checks primary key existence and replica identity settings.
 * Tables must have a primary key and a suitable replica identity
 * (DEFAULT with PK, or FULL) for the CDC framework to capture
 * updates and deletes reliably.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class ReplicaIdentityValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaIdentityValidator.class);

    /**
     * Validates the given table metadata for CDC suitability.
     *
     * @param metadata the table metadata to validate
     * @throws InvalidTableException if the table is not suitable for CDC
     */
    public void validate(TableMetadata metadata) {
        if (metadata.primaryKeyColumns().isEmpty()) {
            throw new InvalidTableException(
                    "Table " + metadata.tableId() + " has no primary key. " +
                    "CDC requires a stable primary key for row identification.");
        }

        String identity = metadata.replicaIdentity();
        switch (identity) {
            case "NOTHING" -> throw new InvalidTableException(
                    "Table " + metadata.tableId() + " has NOTHING replica identity. " +
                    "Cannot capture UPDATE/DELETE without row identity.");

            case "FULL" -> LOG.warn("Table {} uses REPLICA IDENTITY FULL — " +
                    "supported but less efficient than DEFAULT with a primary key",
                    metadata.tableId());

            case "DEFAULT" -> LOG.debug("Table {} has DEFAULT replica identity with PK — optimal for CDC",
                    metadata.tableId());

            case "INDEX" -> LOG.debug("Table {} has INDEX replica identity — supported",
                    metadata.tableId());

            default -> LOG.warn("Table {} has unknown replica identity '{}' — proceeding with caution",
                    metadata.tableId(), identity);
        }
    }
}
