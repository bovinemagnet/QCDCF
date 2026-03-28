package com.paulsnow.qcdcf.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChangeEnvelopeTest {

    private static final TableId TABLE = new TableId("public", "customer");
    private static final RowKey KEY = new RowKey(Map.of("id", 42));
    private static final SourcePosition POSITION = new SourcePosition(12345L, Instant.now());

    private ChangeEnvelope createEnvelope() {
        return new ChangeEnvelope(
                UUID.randomUUID(),
                "test-connector",
                TABLE,
                OperationType.INSERT,
                CaptureMode.LOG,
                KEY,
                null,
                Map.of("name", "Alice"),
                POSITION,
                Instant.now(),
                null,
                Map.of("trace-id", "abc-123")
        );
    }

    @Test
    void createsValidEnvelope() {
        var envelope = createEnvelope();
        assertThat(envelope.connectorId()).isEqualTo("test-connector");
        assertThat(envelope.tableId()).isEqualTo(TABLE);
        assertThat(envelope.operation()).isEqualTo(OperationType.INSERT);
        assertThat(envelope.captureMode()).isEqualTo(CaptureMode.LOG);
        assertThat(envelope.key()).isEqualTo(KEY);
    }

    @Test
    void metadataDefaultsToEmptyMap() {
        var envelope = new ChangeEnvelope(
                UUID.randomUUID(), "c", TABLE, OperationType.INSERT,
                CaptureMode.LOG, KEY, null, null, POSITION,
                Instant.now(), null, null
        );
        assertThat(envelope.metadata()).isEmpty();
    }

    @Test
    void afterMapIsDefensivelyCopied() {
        var mutable = new java.util.HashMap<String, Object>();
        mutable.put("name", "Bob");

        var envelope = new ChangeEnvelope(
                UUID.randomUUID(), "c", TABLE, OperationType.INSERT,
                CaptureMode.LOG, KEY, null, mutable, POSITION,
                Instant.now(), null, null
        );

        mutable.put("name", "Eve");
        assertThat(envelope.after().get("name")).isEqualTo("Bob");
    }

    @Test
    void nullEventIdThrows() {
        assertThatThrownBy(() -> new ChangeEnvelope(
                null, "c", TABLE, OperationType.INSERT,
                CaptureMode.LOG, KEY, null, null, POSITION,
                Instant.now(), null, null
        )).isInstanceOf(NullPointerException.class);
    }

    @Test
    void nullKeyThrows() {
        assertThatThrownBy(() -> new ChangeEnvelope(
                UUID.randomUUID(), "c", TABLE, OperationType.INSERT,
                CaptureMode.LOG, null, null, null, POSITION,
                Instant.now(), null, null
        )).isInstanceOf(NullPointerException.class);
    }
}
