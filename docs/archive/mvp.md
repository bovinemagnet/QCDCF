# MVP Strategy for a PostgreSQL CDC Framework in Quarkus

## Purpose

This document is a companion to the PRD and Technical Architecture Specification.

Its purpose is to keep the first implementation small, useful, and shippable while preserving a clean path to a more durable event backbone such as Kafka or RabbitMQ later.

The key idea is simple:

* use **PostgreSQL-first CDC**,
* use **Quarkus internal messaging** for the in-process pipeline,
* define a **stable event envelope** from day one,
* place all delivery behind a **sink abstraction**,
* defer durable broker complexity until the CDC algorithm is proven.

This keeps the MVP focused on the real hard problem: correctly combining live change capture with online snapshotting.

---

## What this MVP is trying to prove

The MVP is not trying to prove every part of the future platform.

It is trying to prove that we can:

1. read committed row changes from PostgreSQL,
2. perform chunked table reads,
3. create low/high watermark boundaries,
4. reconcile snapshot rows with in-flight log changes correctly,
5. publish a stable internal event model,
6. restart safely enough for development and early testing.

If those parts work, the rest of the platform becomes engineering and hardening work rather than research.

---

## Guiding principles

### 1. Keep the source side real

The source side should be implemented in a way that is close to production:

* PostgreSQL logical decoding,
* publication,
* replication slot,
* watermark table,
* chunked primary-key scans.

Do not fake the source side too much, because correctness depends on real source ordering and overlap behavior.

### 2. Keep the sink side replaceable

The output side should be easy to swap:

* internal sink first,
* Kafka later,
* RabbitMQ later if needed.

This means the CDC core must not depend on Kafka APIs or RabbitMQ APIs.

### 3. Prove correctness before platform breadth

The hardest and most valuable part is watermark reconciliation.

Do not spend early time on:

* UI,
* multi-database support,
* advanced operations,
* exactly-once delivery,
* rich control planes.

### 4. Prefer plain Java core with Quarkus at the shell

The CDC engine should be testable without CDI, HTTP, or broker infrastructure.

Use Quarkus for:

* configuration,
* lifecycle,
* health,
* REST endpoints,
* internal messaging channels.

---

## Recommended MVP shape

### In scope

Build these first:

* PostgreSQL source adapter
* WAL/logical decoding reader
* watermark writer using a dedicated source table
* chunked snapshot reader using primary key ordering
* reconciliation engine that removes colliding keys inside a watermark window
* canonical change event envelope
* Quarkus internal messaging pipeline
* in-memory or logging sink
* minimal REST admin endpoints
* minimal metadata persistence for checkpoints and snapshot progress
* integration tests with PostgreSQL

### Out of scope

Do not build these in the MVP:

* Kafka as a mandatory dependency
* RabbitMQ support
* multi-node connector clustering
* non-PostgreSQL databases
* complex scheduling
* GUI administration console
* generalized plugin marketplace style architecture
* schema registry integration
* exactly-once semantics
* non-primary-key tables

---

## Why internal messaging is enough to start

Internal Quarkus messaging is sufficient for the MVP because it gives us:

* asynchronous flow inside the process,
* clean boundaries between pipeline stages,
* back-pressure support,
* testability without standing up a broker,
* freedom to focus on correctness first.

This is enough to validate the design and get quick wins.

However, internal messaging should be treated as an **internal orchestration mechanism**, not the long-term external delivery contract.

That distinction matters.

---

## Why the sink must still be designed for Kafka or RabbitMQ later

Even if the first version publishes only to an in-memory sink, the emitted event model should already assume a future broker-based world.

That means designing for:

* stable event envelopes,
* serialization boundaries,
* explicit publication acknowledgment,
* ordered publication by key,
* retry behavior,
* future dead-letter handling.

The sink implementation can be simple now, but the contract should not be throwaway.

---

## MVP architecture in one sentence

**Use Quarkus internal messaging for the in-process CDC pipeline, and keep final delivery behind a pluggable sink interface so Kafka or RabbitMQ can be added later without rewriting the CDC core.**

---

## Suggested MVP modules

A reduced module layout is enough for the first version.

```text
cdc-platform/
  cdc-api-model/
  cdc-core/
  cdc-postgres/
  cdc-runtime-quarkus/
  cdc-test-kit/
```

### `cdc-api-model`

Contains:

* `ChangeEnvelope`
* `TableId`
* `RowKey`
* `SourcePosition`
* enums such as `OperationType` and `CaptureMode`

### `cdc-core`

Contains:

* watermark coordination
* reconciliation engine
* chunk planning
* checkpoint model
* sink abstraction

### `cdc-postgres`

Contains:

* logical decoding reader
* watermark writer
* snapshot reader
* PostgreSQL metadata reader

### `cdc-runtime-quarkus`

Contains:

* REST API
* Quarkus messaging channels
* configuration
* health
* wiring

### `cdc-test-kit`

Contains:

* integration testing helpers
* conflict scenario tests
* snapshot overlap test scenarios

---

## Suggested internal pipeline

The MVP pipeline should be explicit and small.

```text
WAL Reader
  -> wal-normalized
  -> watermark-window
  -> reconcile-output
  -> publish-request
  -> sink
```

A snapshot flow can feed into the same reconciliation stage:

```text
Snapshot Trigger
  -> write low watermark
  -> read chunk
  -> write high watermark
  -> hand chunk to reconciliation
```

This is enough to prove the architecture without building a large distributed event platform.

---

## Sink abstraction

The sink abstraction is the most important design choice for avoiding rework later.

Suggested interface:

```java
public interface EventSink {
    void publish(ChangeEnvelope event);
    void publishBatch(List<ChangeEnvelope> events);
}
```

First implementations:

* `InMemoryEventSink`
* `LoggingEventSink`
* optional `JsonFileEventSink`

Later implementations:

* `KafkaEventSink`
* `RabbitMqEventSink`

### Design rule

The CDC core should know only about `EventSink`.

It should not know about:

* Kafka topics,
* RabbitMQ exchanges,
* producer APIs,
* broker-specific retry semantics.

Those belong in sink adapters.

---

## Event envelope for the MVP

Define the event envelope now and keep it stable.

```java
public record ChangeEnvelope(
    UUID eventId,
    String connectorId,
    String schemaName,
    String tableName,
    String operation,
    Map<String, Object> key,
    Map<String, Object> before,
    Map<String, Object> after,
    long lsn,
    Instant commitTs,
    String captureMode
) {}
```

This is enough for:

* internal testing,
* future JSON serialization,
* Kafka publishing,
* RabbitMQ publishing,
* downstream assertions.

Avoid over-designing the envelope in the MVP.

---

## Minimal checkpointing strategy

The MVP does not need a huge metadata model, but it should not be stateless.

At minimum persist:

* last published WAL position,
* active snapshot job,
* active chunk boundary,
* low/high watermark ids and their observed positions,
* snapshot completion state.

This can live in a small framework-owned metadata schema.

Do not skip this entirely. Without it, restart behavior becomes too misleading.

---

## Minimal REST API

Only expose the endpoints you need to exercise the MVP.

Suggested endpoints:

* `POST /connectors/{id}/start`
* `POST /connectors/{id}/pause`
* `POST /connectors/{id}/snapshots/table`
* `GET /connectors/{id}/status`
* `GET /connectors/{id}/progress`

Do not build a full management plane yet.

---

## The quickest useful milestone

The fastest meaningful win is not full platform completion.

It is this:

### Milestone 1

* connect to PostgreSQL logical replication,
* read row changes for one table,
* emit `ChangeEnvelope` to `LoggingEventSink`,
* verify ordering and payload shape.

### Milestone 2

* add watermark table,
* trigger a chunked snapshot for one table,
* reconcile conflicts between snapshot rows and concurrent WAL events,
* emit final merged output to sink.

### Milestone 3

* persist minimal checkpoints,
* restart and recover from an interrupted snapshot,
* prove duplicate-safe behavior in tests.

### Milestone 4

* add `KafkaEventSink`,
* keep internal pipeline unchanged,
* validate publication acknowledgments and topic/key mapping.

This sequence produces visible progress quickly.

---

## What to defer deliberately

To avoid architecture bloat, explicitly defer these decisions until the MVP works:

### Broker decision

Do not force Kafka or RabbitMQ into the first cut.

Keep the sink pluggable and choose the first durable broker after the algorithm is working.

### Multi-node execution

Use one active runtime per connector for MVP.

### Advanced operational workflows

Avoid:

* job orchestration dashboards,
* self-service onboarding,
* scheduled campaigns,
* connector balancing.

### Generalized abstraction layers

Do not create deep source or sink plugin hierarchies until PostgreSQL works cleanly.

---

## Suggested coding priorities

Build in this order:

1. canonical event model
2. PostgreSQL metadata reader
3. WAL/logical decoding reader
4. simple internal pipeline using Quarkus messaging
5. logging sink
6. watermark writer
7. chunk snapshot reader
8. reconciliation engine
9. minimal checkpoint persistence
10. Kafka sink adapter

This keeps the work front-loaded on correctness, not infrastructure ceremony.

---

## Suggested test priorities

These tests matter more than almost anything else:

1. update to a row while a snapshot chunk is running
2. multiple updates to the same key between low and high watermark
3. delete after the row is read in a chunk but before high watermark closes
4. connector restart after chunk read but before publish completion
5. connector restart after publish but before checkpoint persistence
6. duplicate replay from last known WAL position

If these pass reliably, the MVP is doing real work.

---

## Exit criteria for the MVP

The MVP is complete when all of the following are true:

* one PostgreSQL table can stream live changes,
* the same table can be snapshot in chunks,
* the watermark mechanism prevents stale snapshot rows from overriding newer log changes,
* restart behavior is good enough for dev and test,
* the internal pipeline does not need to change when swapping from `LoggingEventSink` to `KafkaEventSink`.

That is the real success condition.

---

## Final recommendation

Use internal Quarkus messaging now.

But do it in a way that assumes a real broker later.

That means:

* internal channels for orchestration,
* stable emitted envelope,
* sink abstraction from day one,
* PostgreSQL-first source implementation,
* Kafka added after the algorithm is proven,
* RabbitMQ only if there is a strong reason.

This path gives quick wins, keeps the MVP small, and avoids getting trapped in a large architecture too early.
