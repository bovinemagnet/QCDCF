package com.paulsnow.qcdcf.runtime.kafka;

import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link TopicRouter}.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
class TopicRouterTest {

    @Test
    void routeProducesExpectedTopicName() {
        TopicRouter router = new TopicRouter("qcdcf");
        ChangeEnvelope event = sampleEvent("public", "users");

        String topic = router.route(event);

        assertEquals("qcdcf.public.users", topic);
    }

    @Test
    void routeUsesCustomPrefix() {
        TopicRouter router = new TopicRouter("myapp-cdc");
        ChangeEnvelope event = sampleEvent("inventory", "products");

        String topic = router.route(event);

        assertEquals("myapp-cdc.inventory.products", topic);
    }

    @Test
    void routeHandlesDifferentSchemas() {
        TopicRouter router = new TopicRouter("prefix");
        ChangeEnvelope event = sampleEvent("sales", "orders");

        String topic = router.route(event);

        assertEquals("prefix.sales.orders", topic);
    }

    private static ChangeEnvelope sampleEvent(String schema, String table) {
        return new ChangeEnvelope(
                UUID.randomUUID(),
                "test-connector",
                new TableId(schema, table),
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
