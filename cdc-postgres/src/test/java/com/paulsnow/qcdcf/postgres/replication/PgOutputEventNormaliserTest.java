package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PgOutputEventNormaliserTest {

    private static final TableId TABLE = new TableId("public", "customer");
    private static final List<String> KEY_COLUMNS = List.of("id");
    private PgOutputEventNormaliser normaliser;

    @BeforeEach
    void setUp() {
        normaliser = new PgOutputEventNormaliser("test-connector");
    }

    @Test
    void normalisesInsertMessage() {
        var message = new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.INSERT,
                1000L, 42L, Instant.now(),
                TABLE, OperationType.INSERT,
                null, Map.of("id", 1, "name", "Alice"),
                KEY_COLUMNS
        );

        ChangeEnvelope envelope = normaliser.normalise(message);

        assertThat(envelope.connectorId()).isEqualTo("test-connector");
        assertThat(envelope.tableId()).isEqualTo(TABLE);
        assertThat(envelope.operation()).isEqualTo(OperationType.INSERT);
        assertThat(envelope.captureMode()).isEqualTo(CaptureMode.LOG);
        assertThat(envelope.key().columns()).containsEntry("id", 1);
        assertThat(envelope.after()).containsEntry("name", "Alice");
        assertThat(envelope.before()).isNull();
        assertThat(envelope.position().lsn()).isEqualTo(1000L);
        assertThat(envelope.position().txId()).isEqualTo(42L);
    }

    @Test
    void normalisesUpdateMessage() {
        var message = new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.UPDATE,
                2000L, 43L, Instant.now(),
                TABLE, OperationType.UPDATE,
                Map.of("id", 1, "name", "Alice"),
                Map.of("id", 1, "name", "Bob"),
                KEY_COLUMNS
        );

        ChangeEnvelope envelope = normaliser.normalise(message);

        assertThat(envelope.operation()).isEqualTo(OperationType.UPDATE);
        assertThat(envelope.before()).containsEntry("name", "Alice");
        assertThat(envelope.after()).containsEntry("name", "Bob");
        assertThat(envelope.key().columns()).containsEntry("id", 1);
    }

    @Test
    void normalisesDeleteMessage() {
        var message = new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.DELETE,
                3000L, 44L, Instant.now(),
                TABLE, OperationType.DELETE,
                Map.of("id", 1), null,
                KEY_COLUMNS
        );

        ChangeEnvelope envelope = normaliser.normalise(message);

        assertThat(envelope.operation()).isEqualTo(OperationType.DELETE);
        assertThat(envelope.before()).containsEntry("id", 1);
        assertThat(envelope.after()).isNull();
        assertThat(envelope.key().columns()).containsEntry("id", 1);
    }

    @Test
    void extractsCompositeKey() {
        var keyColumns = List.of("tenant_id", "record_id");
        var message = new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.INSERT,
                4000L, 45L, Instant.now(),
                TABLE, OperationType.INSERT,
                null, Map.of("tenant_id", 10, "record_id", 20, "data", "value"),
                keyColumns
        );

        ChangeEnvelope envelope = normaliser.normalise(message);

        assertThat(envelope.key().columns())
                .containsEntry("tenant_id", 10)
                .containsEntry("record_id", 20)
                .doesNotContainKey("data");
    }

    @Test
    void setsCaptureModeToLog() {
        var message = new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.INSERT,
                5000L, 46L, Instant.now(),
                TABLE, OperationType.INSERT,
                null, Map.of("id", 1),
                KEY_COLUMNS
        );

        ChangeEnvelope envelope = normaliser.normalise(message);

        assertThat(envelope.captureMode()).isEqualTo(CaptureMode.LOG);
        assertThat(envelope.watermark()).isNull();
    }

    @Test
    void generatesUniqueEventId() {
        var message = new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.INSERT,
                6000L, 47L, Instant.now(),
                TABLE, OperationType.INSERT,
                null, Map.of("id", 1),
                KEY_COLUMNS
        );

        ChangeEnvelope first = normaliser.normalise(message);
        ChangeEnvelope second = normaliser.normalise(message);

        assertThat(first.eventId()).isNotEqualTo(second.eventId());
    }

    @Test
    void rejectsNonDataMessage() {
        var message = new DecodedReplicationMessage(
                DecodedReplicationMessage.MessageType.BEGIN,
                7000L, 48L, Instant.now(),
                null, null,
                null, null, null
        );

        assertThatThrownBy(() -> normaliser.normalise(message))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
