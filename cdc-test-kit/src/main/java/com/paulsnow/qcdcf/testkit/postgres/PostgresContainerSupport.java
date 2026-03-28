package com.paulsnow.qcdcf.testkit.postgres;

import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Provides a shared PostgreSQL Testcontainer configured for logical replication.
 * <p>
 * Uses {@code postgres:16-alpine} with {@code wal_level=logical} enabled.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public final class PostgresContainerSupport {

    private static final String IMAGE = "postgres:16-alpine";

    private PostgresContainerSupport() {}

    /**
     * Creates a new PostgreSQL container configured for CDC testing.
     * The container has logical replication enabled and uses the {@code qcdcf_test} database.
     *
     * @return a configured but not yet started container
     */
    @SuppressWarnings("resource")
    public static PostgreSQLContainer<?> createContainer() {
        return new PostgreSQLContainer<>(IMAGE)
                .withDatabaseName("qcdcf_test")
                .withUsername("qcdcf")
                .withPassword("qcdcf")
                .withCommand("postgres",
                        "-c", "wal_level=logical",
                        "-c", "max_replication_slots=4",
                        "-c", "max_wal_senders=4");
    }
}
