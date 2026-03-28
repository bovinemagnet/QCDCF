package com.paulsnow.qcdcf.runtime.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EventSerializer}.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
class EventSerializerTest {

    private final EventSerializer serialiser = new EventSerializer();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void serialiseProducesValidJson() throws Exception {
        ChangeEnvelope event = sampleEvent();

        byte[] bytes = serialiser.serialise(event);

        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        // Parse to verify it is valid JSON
        JsonNode node = mapper.readTree(bytes);
        assertNotNull(node);
        assertEquals("test-connector", node.get("connectorId").asText());
    }

    @Test
    void serialiseIncludesTableId() throws Exception {
        ChangeEnvelope event = sampleEvent();

        byte[] bytes = serialiser.serialise(event);
        JsonNode node = mapper.readTree(bytes);

        JsonNode tableId = node.get("tableId");
        assertNotNull(tableId);
        assertEquals("public", tableId.get("schema").asText());
        assertEquals("users", tableId.get("table").asText());
    }

    @Test
    void serialiseWritesTimestampsAsIsoFormat() throws Exception {
        Instant timestamp = Instant.parse("2026-03-19T10:30:00Z");
        ChangeEnvelope event = new ChangeEnvelope(
                UUID.randomUUID(),
                "test-connector",
                new TableId("public", "users"),
                OperationType.INSERT,
                CaptureMode.LOG,
                new RowKey(Map.of("id", 1)),
                null,
                Map.of("name", "test"),
                new SourcePosition(1L, timestamp),
                timestamp,
                null,
                Map.of()
        );

        byte[] bytes = serialiser.serialise(event);
        JsonNode node = mapper.readTree(bytes);

        String captureTs = node.get("captureTimestamp").asText();
        // Should be ISO-8601 format, not a numeric timestamp
        assertTrue(captureTs.contains("2026"), "Timestamp should be ISO format, got: " + captureTs);
        assertFalse(captureTs.matches("^\\d+$"), "Timestamp should not be a plain number");
    }

    @Test
    void serialiseIncludesOperation() throws Exception {
        ChangeEnvelope event = sampleEvent();

        byte[] bytes = serialiser.serialise(event);
        JsonNode node = mapper.readTree(bytes);

        assertEquals("INSERT", node.get("operation").asText());
    }

    @Test
    void serialiseIncludesRowKey() throws Exception {
        ChangeEnvelope event = sampleEvent();

        byte[] bytes = serialiser.serialise(event);
        JsonNode node = mapper.readTree(bytes);

        JsonNode key = node.get("key");
        assertNotNull(key);
        JsonNode columns = key.get("columns");
        assertNotNull(columns);
        assertEquals(1, columns.get("id").asInt());
    }

    private static ChangeEnvelope sampleEvent() {
        return new ChangeEnvelope(
                UUID.randomUUID(),
                "test-connector",
                new TableId("public", "users"),
                OperationType.INSERT,
                CaptureMode.LOG,
                new RowKey(Map.of("id", 1)),
                null,
                Map.of("name", "test"),
                new SourcePosition(1L, Instant.now()),
                Instant.now(),
                null,
                Map.of()
        );
    }
}
