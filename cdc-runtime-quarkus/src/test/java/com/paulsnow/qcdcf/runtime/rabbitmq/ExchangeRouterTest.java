package com.paulsnow.qcdcf.runtime.rabbitmq;

import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;

class ExchangeRouterTest {
    private final ExchangeRouter router = new ExchangeRouter("qcdcf");

    @Test
    void routingKeyUsesSchemaAndTable() {
        var event = sampleEvent("public", "customer");
        assertThat(router.routingKey(event)).isEqualTo("public.customer");
    }

    @Test
    void routingKeyWithNonDefaultSchema() {
        var event = sampleEvent("sales", "orders");
        assertThat(router.routingKey(event)).isEqualTo("sales.orders");
    }

    @Test
    void exchangeNameFromConstructor() {
        assertThat(router.exchangeName()).isEqualTo("qcdcf");
    }

    private ChangeEnvelope sampleEvent(String schema, String table) {
        return new ChangeEnvelope(UUID.randomUUID(), "test", new TableId(schema, table),
                OperationType.INSERT, CaptureMode.LOG, new RowKey(Map.of("id", 1)),
                null, Map.of("id", 1), new SourcePosition(1000L, Instant.now()),
                Instant.now(), null, Map.of());
    }
}
