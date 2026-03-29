# RabbitMQ Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a RabbitMQ `EventSink` implementation so CDC events can be published to RabbitMQ exchanges, selectable via `qcdcf.sink.type=rabbitmq`.

**Architecture:** New `rabbitmq` package mirroring the existing `kafka` package. `RabbitMQEventSink` uses the `amqp-client` library directly (no Quarkus reactive messaging). `ExchangeRouter` maps events to exchange + routing key. Reuses the existing `EventSerializer` from the kafka package. Wired into `EventSinkProducer` via a new switch case.

**Tech Stack:** Java 25, RabbitMQ amqp-client 5.22.0, Jackson (existing)

**Spec:** `docs/superpowers/specs/2026-03-29-rabbitmq-sink-design.md`

---

## File Structure

### New Files
| File | Responsibility |
|------|---------------|
| `cdc-runtime-quarkus/.../rabbitmq/RabbitMQEventSink.java` | EventSink publishing to RabbitMQ via amqp-client |
| `cdc-runtime-quarkus/.../rabbitmq/ExchangeRouter.java` | Maps events to exchange name + routing key |
| `cdc-runtime-quarkus/src/test/.../rabbitmq/ExchangeRouterTest.java` | Unit tests for routing key generation |
| `cdc-runtime-quarkus/src/test/.../rabbitmq/RabbitMQEventSinkTest.java` | Unit tests with mocked Channel |

### Modified Files
| File | Changes |
|------|---------|
| `cdc-runtime-quarkus/build.gradle` | Add amqp-client dependency |
| `cdc-runtime-quarkus/.../config/ConnectorRuntimeConfig.java` | Add RabbitMQConfig interface |
| `cdc-runtime-quarkus/.../service/EventSinkProducer.java` | Add "rabbitmq" case |
| `cdc-runtime-quarkus/src/main/resources/application.properties` | Add rabbitmq defaults |

---

## Task 1: Add amqp-client Dependency and RabbitMQ Configuration

**Files:**
- Modify: `cdc-runtime-quarkus/build.gradle`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/config/ConnectorRuntimeConfig.java`
- Modify: `cdc-runtime-quarkus/src/main/resources/application.properties`

- [ ] **Step 1: Add amqp-client to build.gradle**

Add to the `dependencies` block:
```groovy
implementation 'com.rabbitmq:amqp-client:5.22.0'
```

- [ ] **Step 2: Add RabbitMQConfig to ConnectorRuntimeConfig**

Read `ConnectorRuntimeConfig.java`. In the existing `SinkConfig` interface, add after `KafkaConfig kafka();`:

```java
/** RabbitMQ-specific sink configuration. */
RabbitMQConfig rabbitmq();

interface RabbitMQConfig {
    @WithDefault("localhost")
    String host();

    @WithDefault("5672")
    int port();

    @WithDefault("guest")
    String username();

    @WithDefault("guest")
    String password();

    @WithDefault("/")
    String virtualHost();

    @WithDefault("qcdcf")
    String exchangeName();

    @WithDefault("topic")
    String exchangeType();
}
```

- [ ] **Step 3: Add default properties**

Append to `application.properties`:
```properties

# RabbitMQ sink (used when sink.type=rabbitmq)
qcdcf.sink.rabbitmq.host=localhost
qcdcf.sink.rabbitmq.port=5672
qcdcf.sink.rabbitmq.username=guest
qcdcf.sink.rabbitmq.password=guest
qcdcf.sink.rabbitmq.virtual-host=/
qcdcf.sink.rabbitmq.exchange-name=qcdcf
qcdcf.sink.rabbitmq.exchange-type=topic
```

- [ ] **Step 4: Compile**

Run: `gradle :cdc-runtime-quarkus:compileJava`

- [ ] **Step 5: Commit**

```bash
git add cdc-runtime-quarkus/build.gradle \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/config/ConnectorRuntimeConfig.java \
       cdc-runtime-quarkus/src/main/resources/application.properties
git commit -m "feat: add RabbitMQ amqp-client dependency and configuration"
```

---

## Task 2: Create ExchangeRouter with Tests

**Files:**
- Create: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/rabbitmq/ExchangeRouter.java`
- Create: `cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/rabbitmq/ExchangeRouterTest.java`

- [ ] **Step 1: Write the test first**

```java
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
        ChangeEnvelope event = new ChangeEnvelope(
                UUID.randomUUID(), "test", new TableId("public", "customer"),
                OperationType.INSERT, CaptureMode.LOG,
                new RowKey(Map.of("id", 1)), null, Map.of("id", 1),
                new SourcePosition(1000L, Instant.now()), Instant.now(), null, Map.of());

        assertThat(router.routingKey(event)).isEqualTo("public.customer");
    }

    @Test
    void routingKeyWithNonDefaultSchema() {
        ChangeEnvelope event = new ChangeEnvelope(
                UUID.randomUUID(), "test", new TableId("sales", "orders"),
                OperationType.UPDATE, CaptureMode.LOG,
                new RowKey(Map.of("id", 1)), null, Map.of("id", 1),
                new SourcePosition(2000L, Instant.now()), Instant.now(), null, Map.of());

        assertThat(router.routingKey(event)).isEqualTo("sales.orders");
    }

    @Test
    void exchangeNameFromConstructor() {
        assertThat(router.exchangeName()).isEqualTo("qcdcf");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gradle :cdc-runtime-quarkus:test --tests '*ExchangeRouterTest*' -i`
Expected: FAIL — class does not exist

- [ ] **Step 3: Create ExchangeRouter**

```java
package com.paulsnow.qcdcf.runtime.rabbitmq;

import com.paulsnow.qcdcf.model.ChangeEnvelope;

/**
 * Routes change events to a RabbitMQ exchange with a table-based routing key.
 * <p>
 * Routing key pattern: {@code {schema}.{table}} (e.g., {@code public.customer}).
 * Uses a topic exchange by default, allowing consumers to bind with wildcard patterns.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class ExchangeRouter {

    private final String exchangeName;

    public ExchangeRouter(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    /**
     * Returns the exchange name.
     */
    public String exchangeName() {
        return exchangeName;
    }

    /**
     * Returns the routing key for the given event.
     *
     * @param event the change event
     * @return routing key in the format {@code schema.table}
     */
    public String routingKey(ChangeEnvelope event) {
        return event.tableId().canonicalName();
    }
}
```

- [ ] **Step 4: Run tests**

Run: `gradle :cdc-runtime-quarkus:test --tests '*ExchangeRouterTest*' -i`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/rabbitmq/ExchangeRouter.java \
       cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/rabbitmq/ExchangeRouterTest.java
git commit -m "feat: add ExchangeRouter for RabbitMQ routing key generation"
```

---

## Task 3: Create RabbitMQEventSink with Tests

**Files:**
- Create: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/rabbitmq/RabbitMQEventSink.java`
- Create: `cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/rabbitmq/RabbitMQEventSinkTest.java`

- [ ] **Step 1: Write the test first**

```java
package com.paulsnow.qcdcf.runtime.rabbitmq;

import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.*;
import com.paulsnow.qcdcf.runtime.kafka.EventSerializer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RabbitMQEventSinkTest {

    private Channel channel;
    private RabbitMQEventSink sink;

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
        sink = new RabbitMQEventSink(channel, new ExchangeRouter("qcdcf"), new EventSerializer(), "topic");
    }

    @Test
    void publishSendsToCorrectExchangeAndRoutingKey() throws Exception {
        PublishResult result = sink.publish(sampleEvent("public", "customer"));

        assertThat(result.isSuccess()).isTrue();
        verify(channel).basicPublish(
                eq("qcdcf"),
                eq("public.customer"),
                any(AMQP.BasicProperties.class),
                any(byte[].class));
    }

    @Test
    void publishReturnsFailureOnIOException() throws Exception {
        doThrow(new IOException("connection lost")).when(channel)
                .basicPublish(anyString(), anyString(), any(), any(byte[].class));

        PublishResult result = sink.publish(sampleEvent("public", "customer"));

        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    void batchFailsOnFirstError() throws Exception {
        doNothing()
                .doThrow(new IOException("connection lost"))
                .when(channel).basicPublish(anyString(), anyString(), any(), any(byte[].class));

        PublishResult result = sink.publishBatch(List.of(
                sampleEvent("public", "customer"),
                sampleEvent("public", "orders")));

        assertThat(result.isSuccess()).isFalse();
        verify(channel, times(2)).basicPublish(anyString(), anyString(), any(), any(byte[].class));
    }

    @Test
    void closeClosesChannel() throws Exception {
        when(channel.getConnection()).thenReturn(mock(com.rabbitmq.client.Connection.class));

        sink.close();

        verify(channel).close();
    }

    private ChangeEnvelope sampleEvent(String schema, String table) {
        return new ChangeEnvelope(
                UUID.randomUUID(), "test", new TableId(schema, table),
                OperationType.INSERT, CaptureMode.LOG,
                new RowKey(Map.of("id", 1)), null, Map.of("id", 1),
                new SourcePosition(1000L, Instant.now()), Instant.now(), null, Map.of());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gradle :cdc-runtime-quarkus:test --tests '*RabbitMQEventSinkTest*' -i`
Expected: FAIL — class does not exist

- [ ] **Step 3: Create RabbitMQEventSink**

```java
package com.paulsnow.qcdcf.runtime.rabbitmq;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.runtime.kafka.EventSerializer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Publishes change events to a RabbitMQ exchange.
 * <p>
 * Uses the AMQP client directly for synchronous, at-least-once delivery.
 * Messages are published with persistent delivery mode. The exchange is
 * declared idempotently on the first publish.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class RabbitMQEventSink implements EventSink {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQEventSink.class);

    private final Channel channel;
    private final ExchangeRouter router;
    private final EventSerializer serialiser;
    private final String exchangeType;
    private volatile boolean exchangeDeclared;

    public RabbitMQEventSink(Channel channel, ExchangeRouter router,
                             EventSerializer serialiser, String exchangeType) {
        this.channel = channel;
        this.router = router;
        this.serialiser = serialiser;
        this.exchangeType = exchangeType;
    }

    @Override
    public PublishResult publish(ChangeEnvelope event) {
        try {
            ensureExchangeDeclared();

            String exchange = router.exchangeName();
            String routingKey = router.routingKey(event);
            byte[] body = serialiser.serialise(event);

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .deliveryMode(2) // persistent
                    .contentType("application/json")
                    .build();

            channel.basicPublish(exchange, routingKey, props, body);
            LOG.debug("Published event {} to exchange {} with routing key {}",
                    event.eventId(), exchange, routingKey);
            return new PublishResult.Success(1);

        } catch (Exception e) {
            LOG.error("RabbitMQ publish failed for event {}: {}",
                    event.eventId(), e.getMessage(), e);
            return new PublishResult.Failure("RabbitMQ publish failed: " + e.getMessage(), e);
        }
    }

    @Override
    public PublishResult publishBatch(List<ChangeEnvelope> events) {
        for (ChangeEnvelope event : events) {
            PublishResult result = publish(event);
            if (!result.isSuccess()) {
                return result;
            }
        }
        return new PublishResult.Success(events.size());
    }

    @Override
    public void close() {
        LOG.info("Closing RabbitMQ channel and connection");
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (channel != null && channel.getConnection() != null && channel.getConnection().isOpen()) {
                channel.getConnection().close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing RabbitMQ resources: {}", e.getMessage());
        }
    }

    private void ensureExchangeDeclared() throws IOException {
        if (!exchangeDeclared) {
            channel.exchangeDeclare(router.exchangeName(), exchangeType, true);
            exchangeDeclared = true;
            LOG.info("Declared RabbitMQ exchange '{}' (type={})", router.exchangeName(), exchangeType);
        }
    }
}
```

- [ ] **Step 4: Run tests**

Run: `gradle :cdc-runtime-quarkus:test --tests '*RabbitMQEventSinkTest*' -i`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/rabbitmq/RabbitMQEventSink.java \
       cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/rabbitmq/RabbitMQEventSinkTest.java
git commit -m "feat: add RabbitMQEventSink with persistent delivery and exchange declaration"
```

---

## Task 4: Wire RabbitMQ into EventSinkProducer

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/EventSinkProducer.java`

- [ ] **Step 1: Read EventSinkProducer.java**

- [ ] **Step 2: Add imports**

```java
import com.paulsnow.qcdcf.runtime.rabbitmq.ExchangeRouter;
import com.paulsnow.qcdcf.runtime.rabbitmq.RabbitMQEventSink;
import com.rabbitmq.client.ConnectionFactory;
```

- [ ] **Step 3: Add "rabbitmq" case to switch**

In `createSink()`, add to the switch statement before `default`:

```java
case "rabbitmq" -> createRabbitMQSink();
```

- [ ] **Step 4: Add createRabbitMQSink() method**

```java
private RabbitMQEventSink createRabbitMQSink() {
    var rmqConfig = config.sink().rabbitmq();
    LOG.info("Using RabbitMQEventSink with host: {}:{}", rmqConfig.host(), rmqConfig.port());

    try {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rmqConfig.host());
        factory.setPort(rmqConfig.port());
        factory.setUsername(rmqConfig.username());
        factory.setPassword(rmqConfig.password());
        factory.setVirtualHost(rmqConfig.virtualHost());

        var connection = factory.newConnection();
        var channel = connection.createChannel();

        return new RabbitMQEventSink(
                channel,
                new ExchangeRouter(rmqConfig.exchangeName()),
                new EventSerializer(),
                rmqConfig.exchangeType()
        );
    } catch (Exception e) {
        throw new IllegalStateException("Failed to create RabbitMQ connection: " + e.getMessage(), e);
    }
}
```

- [ ] **Step 5: Compile and test**

Run: `gradle :cdc-runtime-quarkus:compileJava`
Run: `gradle :cdc-runtime-quarkus:test`

- [ ] **Step 6: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/EventSinkProducer.java
git commit -m "feat: wire RabbitMQ sink into EventSinkProducer factory"
```

---

## Task 5: Final Verification

- [ ] **Step 1: Run all tests**

Run: `gradle :cdc-api-model:test :cdc-core:test :cdc-runtime-quarkus:test`

- [ ] **Step 2: Verify Antora builds**

Run: `gradle antora`

- [ ] **Step 3: Commit any fixes**
