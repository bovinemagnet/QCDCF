package com.paulsnow.qcdcf.postgres.checkpoint;

import com.paulsnow.qcdcf.core.checkpoint.ConnectorCheckpoint;
import com.paulsnow.qcdcf.model.SourcePosition;
import com.paulsnow.qcdcf.testkit.postgres.PostgresContainerSupport;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class PostgresCheckpointRepositoryIT {

    @Container
    static final PostgreSQLContainer<?> postgres = PostgresContainerSupport.createContainer();

    private Connection connection;
    private PostgresCheckpointRepository repository;

    @BeforeAll
    static void createSchema() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute(PostgresCheckpointRepository.CREATE_TABLE_SQL);
        }
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        repository = new PostgresCheckpointRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    @Test
    void saveAndLoadCheckpoint() {
        var checkpoint = new ConnectorCheckpoint(
                "test-connector",
                new SourcePosition(12345L, 42L, Instant.now()),
                null, -1, Instant.now()
        );

        repository.save(checkpoint);
        Optional<ConnectorCheckpoint> loaded = repository.load("test-connector");

        assertThat(loaded).isPresent();
        assertThat(loaded.get().connectorId()).isEqualTo("test-connector");
        assertThat(loaded.get().position().lsn()).isEqualTo(12345L);
        assertThat(loaded.get().position().txId()).isEqualTo(42L);
    }

    @Test
    void loadNonExistentReturnsEmpty() {
        Optional<ConnectorCheckpoint> loaded = repository.load("non-existent");
        assertThat(loaded).isEmpty();
    }

    @Test
    void saveUpdatesExistingCheckpoint() {
        var first = new ConnectorCheckpoint(
                "update-test",
                new SourcePosition(100L, 1L, Instant.now()),
                null, -1, Instant.now()
        );
        repository.save(first);

        var second = new ConnectorCheckpoint(
                "update-test",
                new SourcePosition(200L, 2L, Instant.now()),
                "public.customer", 3, Instant.now()
        );
        repository.save(second);

        Optional<ConnectorCheckpoint> loaded = repository.load("update-test");
        assertThat(loaded).isPresent();
        assertThat(loaded.get().position().lsn()).isEqualTo(200L);
        assertThat(loaded.get().snapshotTableName()).isEqualTo("public.customer");
        assertThat(loaded.get().snapshotChunkIndex()).isEqualTo(3);
    }

    @Test
    void saveWithNullTxId() {
        var checkpoint = new ConnectorCheckpoint(
                "null-txid-test",
                new SourcePosition(500L, Instant.now()),
                null, -1, Instant.now()
        );

        repository.save(checkpoint);
        Optional<ConnectorCheckpoint> loaded = repository.load("null-txid-test");

        assertThat(loaded).isPresent();
        assertThat(loaded.get().position().txId()).isNull();
        assertThat(loaded.get().position().lsn()).isEqualTo(500L);
    }

    @Test
    void saveWithSnapshotProgress() {
        var checkpoint = new ConnectorCheckpoint(
                "snapshot-test",
                new SourcePosition(1000L, 10L, Instant.now()),
                "public.orders", 5, Instant.now()
        );

        repository.save(checkpoint);
        Optional<ConnectorCheckpoint> loaded = repository.load("snapshot-test");

        assertThat(loaded).isPresent();
        assertThat(loaded.get().snapshotTableName()).isEqualTo("public.orders");
        assertThat(loaded.get().snapshotChunkIndex()).isEqualTo(5);
    }
}
