# RabbitMQ Sink Design

**Date:** 2026-03-29
**Status:** Approved
**Author:** Paul Snow

## Context

QCDCF currently supports two event sinks: `LoggingEventSink` (development) and `KafkaEventSink` (production). RabbitMQ is a widely-used message broker and a natural second production sink. The `EventSink` interface and `EventSinkProducer` factory are already designed for pluggable sinks — adding RabbitMQ follows the established Kafka pattern.

## Design

### New Package: `com.paulsnow.qcdcf.runtime.rabbitmq`

Mirror the existing `kafka` package structure:

**`RabbitMQEventSink`** — implements `EventSink`
- Constructor takes `Channel`, `ExchangeRouter`, `EventSerializer`
- `publish()`: route event to exchange/routing-key, serialise to JSON bytes, call `channel.basicPublish()` with persistent delivery mode (`MessageProperties.PERSISTENT_TEXT_PLAIN`)
- Synchronous publish for at-least-once semantics (matching Kafka pattern)
- `publishBatch()`: publish each event individually, fail-fast on first error (matching Kafka pattern)
- `close()`: close channel and connection
- On first publish, declare the exchange idempotently via `channel.exchangeDeclare()`. Do not declare queues — that is the consumer's responsibility.

**`ExchangeRouter`** — routes events to exchange name + routing key
- Exchange name from config (default: `qcdcf`)
- Routing key pattern: `{schema}.{table}` (e.g., `public.customer`)
- Uses topic exchange type by default (allows wildcard binding on consumer side)

**`EventSerializer`** — reuse the existing `com.paulsnow.qcdcf.runtime.kafka.EventSerializer`. It is a standalone class with no Kafka dependencies (just Jackson). Reference it directly rather than duplicating.

### Configuration

Add `RabbitMQConfig` to `SinkConfig` in `ConnectorRuntimeConfig`:

```java
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

Sink selection: `qcdcf.sink.type=rabbitmq`

### Wiring

Add `"rabbitmq"` case to `EventSinkProducer`'s switch statement:
1. Create `ConnectionFactory` from config
2. Create `Connection` and `Channel`
3. Create `ExchangeRouter` with exchange name from config
4. Create `RabbitMQEventSink` with channel, router, and shared `EventSerializer`
5. Wrap with `RetryingEventSink` (same as Kafka)

### Dependency

Add to `cdc-runtime-quarkus/build.gradle`:
```groovy
implementation 'com.rabbitmq:amqp-client:5.22.0'
```

### Files

**New:**
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/rabbitmq/RabbitMQEventSink.java`
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/rabbitmq/ExchangeRouter.java`
- `cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/rabbitmq/ExchangeRouterTest.java`
- `cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/rabbitmq/RabbitMQEventSinkTest.java`

**Modified:**
- `cdc-runtime-quarkus/build.gradle` — add amqp-client dependency
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/config/ConnectorRuntimeConfig.java` — add RabbitMQConfig
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/EventSinkProducer.java` — add rabbitmq case
- `cdc-runtime-quarkus/src/main/resources/application.properties` — add rabbitmq defaults

### Error Handling

- `IOException` from `basicPublish()` → return `PublishResult.Failure`
- `AlreadyClosedException` → return `PublishResult.Failure` (connection lost)
- The `RetryingEventSink` wrapper handles transient failures with exponential backoff
- Exchange declaration failure on first publish → propagate as `PublishResult.Failure`

## Verification

- `gradle :cdc-runtime-quarkus:test` passes (unit tests with mocked Channel)
- `ExchangeRouterTest`: routing key generation for various table names
- `RabbitMQEventSinkTest`: publish calls, error handling, batch fail-fast, close lifecycle
- With `sink.type=rabbitmq` and a running RabbitMQ instance, events appear on the exchange
