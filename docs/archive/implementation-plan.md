# Implementation Plan for the PostgreSQL CDC MVP

## Purpose

This document translates the MVP strategy into a practical build plan.

It is intentionally biased toward:

* fast feedback,
* quick wins,
* minimal throwaway work,
* correctness-first implementation,
* a clean path to Kafka or RabbitMQ later.

It is not a project management artifact for every future feature. It is a delivery plan for getting the first useful version running.

---

## Implementation philosophy

The implementation should follow four rules:

### 1. Prove the algorithm before the platform

The first success is not a polished service.

The first success is proving that:

* WAL events can be read,
* chunked snapshot rows can be read,
* watermark windows can be created,
* conflicts can be reconciled correctly.

### 2. Keep the code paths narrow

For the first version:

* support PostgreSQL only,
* support one connector instance at a time,
* support one table first,
* support one active snapshot window per table,
* support one sink abstraction with a simple implementation.

### 3. Make every milestone runnable

Every milestone should produce something you can execute, observe, and test.

Avoid long periods of architectural setup without a running result.

### 4. Delay durability and broker complexity until the core works

Use internal Quarkus messaging first.

Do not let Kafka, RabbitMQ, or rich control-plane requirements slow down the first working version.

---

## Target outcome

At the end of this plan, the system should be able to:

* stream row changes from PostgreSQL,
* trigger a table snapshot,
* reconcile overlapping live changes and snapshot rows,
* emit a stable `ChangeEnvelope`,
* persist enough checkpoint state to resume basic work,
* switch from a simple sink to Kafka without rewriting the CDC core.

---

## Recommended delivery phases

The plan is divided into six phases.

1. project bootstrap
2. live WAL capture
3. snapshot and watermark core
4. checkpointing and restart safety
5. operational shell
6. external sink integration

Each phase should end with a demonstrable outcome.

---

## Phase 0: repository and project bootstrap

### Goal

Create a small but clean multi-module project and get the basic developer workflow working.

### Deliverables

* Gradle multi-module project
* Java 21 toolchain
* Quarkus runtime module
* Testcontainers setup
* local PostgreSQL integration test profile
* code formatting and test conventions

### Suggested module layout

```text
cdc-platform/
  cdc-api-model/
  cdc-core/
  cdc-postgres/
  cdc-runtime-quarkus/
  cdc-test-kit/
```

### Tasks

#### Project setup

* create root Gradle build
* define common Java toolchain and JUnit config
* add Quarkus plugin only to runtime module
* add shared dependency management

#### Base dependencies

At minimum:

* JUnit 5
* AssertJ or equivalent
* Testcontainers PostgreSQL
* Quarkus REST
* Quarkus JDBC PostgreSQL
* Quarkus health
* Quarkus config support

#### Test harness bootstrap

* create PostgreSQL container helper
* create a test SQL bootstrap script
* verify Quarkus integration tests can talk to the container

### Exit criteria

* `./gradlew test` passes
* Quarkus app starts locally
* an integration test can initialize a PostgreSQL container and run SQL against it

---

## Phase 1: canonical model and PostgreSQL metadata discovery

### Goal

Create the stable internal event model and enough PostgreSQL metadata support to reason about tables safely.

### Why this comes first

Everything else depends on:

* knowing the table identity,
* knowing the primary key,
* having a stable event envelope.

### Deliverables

* canonical `ChangeEnvelope`
* `TableId`, `RowKey`, `SourcePosition`
* `OperationType`, `CaptureMode`
* PostgreSQL metadata reader
* primary-key discovery support
* replica identity validation

### Classes to implement

#### `cdc-api-model`

* `TableId`
* `RowKey`
* `SourcePosition`
* `ChangeEnvelope`
* `OperationType`
* `CaptureMode`

#### `cdc-postgres`

* `PostgresTableMetadataReader`
* `TableMetadata`
* `ColumnMetadata`
* `ReplicaIdentityValidator`

### Suggested order

1. define the core record types
2. write table metadata queries for PK discovery
3. add replica identity checks
4. write tests against real PostgreSQL tables

### Tests

* primary key discovery for single-column key
* primary key discovery for composite key
* failure for tables without a primary key in MVP mode
* correct schema/table name normalization

### Exit criteria

* metadata reader returns correct PK info from real PostgreSQL
* event envelope types are stable and used consistently in tests

---

## Phase 2: live WAL capture for one table

### Goal

Get real change events flowing from PostgreSQL into the application.

### Important constraint

Do not add snapshot logic yet.

This phase is only about proving live CDC from PostgreSQL.

### Deliverables

* PostgreSQL logical replication reader
* publication and slot support
* raw replication event decoding
* event normalization into `ChangeEnvelope`
* simple sink that logs or stores emitted events in memory

### Classes to implement

#### `cdc-postgres`

* `PostgresSourceAdapter`
* `PostgresLogicalReplicationClient`
* `PostgresLogStreamReader`
* `PgOutputMessageDecoder`
* `PgOutputEventNormalizer`

#### `cdc-core`

* `EventSink`
* `LoggingEventSink`
* optional `InMemoryEventSink`

#### `cdc-runtime-quarkus`

* minimal bootstrap service to start log reading
* minimal configuration binding

### Suggested order

1. establish source database prerequisites manually

   * publication
   * logical replication slot
2. build raw replication connection
3. decode row change messages
4. normalize into `ChangeEnvelope`
5. send to logging sink
6. add an integration test that inserts, updates, and deletes rows

### Tests

* insert produces an `INSERT` event
* update produces an `UPDATE` event
* delete produces a `DELETE` event
* events arrive in commit order for a simple scenario
* table filtering works as expected

### Exit criteria

* a real PostgreSQL table can emit row changes into `LoggingEventSink`
* at least one integration test proves insert/update/delete flow end to end

### Quick win milestone

At this point, you already have a useful and demonstrable internal CDC stream.

That is your first visible win.

---

## Phase 3: internal pipeline using Quarkus messaging

### Goal

Introduce internal asynchronous boundaries without adding broker complexity.

### Why now

By this point you have real WAL events.

Now you can shape the processing pipeline while keeping it in-process.

### Deliverables

* internal Quarkus messaging channels
* explicit processing stages
* controlled handoff between stages
* sink abstraction unchanged

### Suggested channels

* `wal-raw`
* `wal-normalized`
* `reconcile-input`
* `publish-request`

Do not create too many stages initially. Keep the graph simple.

### Classes to implement

#### `cdc-runtime-quarkus`

* `WalIngressChannelBridge`
* `NormalizedEventProcessor`
* `PublishRequestProcessor`

These names can change, but the point is to separate responsibilities without over-fragmenting the pipeline.

### Suggested order

1. wire one internal channel from WAL reader to event processor
2. route normalized events to publish request stage
3. call existing sink implementation from the publish stage
4. verify the pipeline still behaves identically to the direct path

### Tests

* internal channel flow preserves event ordering for simple cases
* sink still receives expected events
* pipeline can be paused or disabled via config in tests

### Exit criteria

* live events flow through internal Quarkus messaging rather than direct method calls
* no external broker is required

---

## Phase 4: watermark table and chunked snapshot reading

### Goal

Add the second half of the product: online table reads coordinated with watermarks.

### Deliverables

* watermark table support
* watermark writer
* chunk planning
* snapshot reader with keyset pagination
* one-table snapshot job flow

### Classes to implement

#### `cdc-postgres`

* `PostgresWatermarkWriter`
* `PostgresSnapshotReader`
* `PostgresChunkSqlBuilder`

#### `cdc-core`

* `SnapshotOptions`
* `SnapshotChunkPlan`
* `SnapshotChunkResult`
* `ChunkPlanner`
* `DefaultChunkPlanner`
* `SnapshotCoordinator`
* `DefaultSnapshotCoordinator`

### Suggested order

1. create dedicated watermark table in source DB
2. implement low/high watermark writes
3. build keyset chunk query generation
4. read one chunk from one table by PK order
5. expose a simple snapshot trigger path
6. verify watermark rows appear in WAL output

### Tests

* watermark low/high writes are visible in the replication stream
* chunk reader returns rows ordered by PK
* chunk reader resumes from last seen key
* snapshot trigger reads first chunk successfully

### Exit criteria

* the app can trigger a chunk read for one table
* watermark events and chunk rows can both be observed during a test

### Quick win milestone

You now have both input streams needed for the DBLog-style behavior:

* live WAL events
* chunk snapshot rows

---

## Phase 5: reconciliation engine

### Goal

Implement the actual overlap handling between snapshot rows and live WAL events.

### Why this is the core milestone

This is the reason the project exists.

If this phase works, the MVP becomes genuinely valuable.

### Deliverables

* watermark window model
* collision detection by row key
* surviving snapshot rows after collision removal
* ordered emission of live events plus surviving snapshot rows

### Classes to implement

#### `cdc-core`

* `WatermarkWindow`
* `WatermarkContext`
* `WatermarkCoordinator`
* `DefaultWatermarkCoordinator`
* `ReconciliationEngine`
* `DefaultReconciliationEngine`
* `ReconciliationResult`

### Suggested algorithm for MVP

For a single window on one table:

1. open window metadata after low watermark is written
2. hold snapshot chunk rows keyed by primary key
3. observe log events between low and high watermark
4. remove any snapshot row whose key appears in a log event
5. when high watermark arrives:

   * emit log events through the high watermark
   * emit remaining snapshot rows
   * close the window

This keeps the first version simple and aligned with the intended design.

### Suggested order

1. build an in-memory reconciliation engine first
2. feed a synthetic stream of events into it in unit tests
3. connect it to real WAL + snapshot inputs
4. emit merged output to the existing sink

### Tests

These are the highest-value tests in the entire project:

* snapshot row survives when no live change overlaps
* snapshot row is removed when the same key is updated inside the window
* snapshot row is removed when the same key is deleted inside the window
* multiple updates to the same key inside the window preserve live event order
* unrelated keys do not affect each other

### Exit criteria

* reconciliation works correctly for one table in integration tests
* stale snapshot rows do not override newer live changes

### Major milestone

This is the first point at which the MVP demonstrates the central DBLog-inspired value proposition.

---

## Phase 6: minimal checkpoint persistence and restart safety

### Goal

Persist enough state to avoid a misleading toy implementation.

### Deliverables

* minimal metadata schema
* last published WAL position
* active snapshot job record
* active chunk state
* watermark window state
* basic recovery on restart

### Classes to implement

#### `cdc-core`

* `ConnectorCheckpoint`
* `CheckpointManager`
* `SnapshotProgress`

#### `cdc-runtime-quarkus` or dedicated metadata package if extracted later

* `CheckpointRepository`
* `SnapshotJobRepository`
* `WatermarkWindowRepository`

### Suggested order

1. persist last published WAL position
2. persist active snapshot chunk boundaries
3. persist watermark window metadata
4. reload minimal state on startup
5. verify restart behavior in integration tests

### Tests

* restart after WAL events continue from last persisted position
* restart during active snapshot does not lose chunk progress
* duplicate-safe handling from replayed position is acceptable for MVP

### Exit criteria

* the app can restart without pretending all work must begin from zero
* checkpoint state is visible and inspectable in the metadata DB

---

## Phase 7: minimal REST and operations shell

### Goal

Make the MVP operable without creating a heavy control plane.

### Deliverables

* start connector endpoint
* pause connector endpoint
* trigger table snapshot endpoint
* status endpoint
* progress endpoint
* health endpoint

### Classes to implement

#### `cdc-runtime-quarkus`

* `ConnectorResource`
* `SnapshotResource` or a combined resource
* `ConnectorStatusService`
* health checks

### Suggested order

1. add connector start endpoint
2. add snapshot trigger endpoint
3. add status/progress read endpoints
4. add readiness/liveness checks

### Tests

* connector can start from REST
* snapshot can be triggered from REST
* health endpoint reports source and metadata availability

### Exit criteria

* a developer can run the service and operate the MVP without touching internals

---

## Phase 8: Kafka sink adapter

### Goal

Add the first durable external sink without changing the CDC core.

### Why this comes late

The algorithm and internal boundaries should already be proven.

Kafka should be a new sink implementation, not a redesign.

### Deliverables

* `KafkaEventSink`
* basic topic routing strategy
* serialization of `ChangeEnvelope`
* publish acknowledgment handling

### Classes to implement

#### new module later if needed, or keep inside runtime initially

* `KafkaEventSink`
* `TopicRouter`
* `EventSerializer`
* optional `PartitionKeyStrategy`

### Suggested order

1. serialize `ChangeEnvelope` to JSON
2. publish to topic-per-table or one test topic
3. return only after producer acknowledgment
4. wire sink by config profile

### Tests

* event can be published to Kafka from the sink
* switching from logging sink to Kafka sink requires no CDC core changes
* checkpoint advancement happens only after publish success

### Exit criteria

* Kafka is an optional sink implementation selected by configuration
* internal messaging pipeline is unchanged

---

## Optional Phase 9: RabbitMQ sink adapter

### Goal

Add RabbitMQ only if required by the surrounding ecosystem.

### Recommendation

Do this only after Kafka or another durable sink is already working.

RabbitMQ should remain a separate sink implementation, not a reason to distort the event model.

---

## Suggested week-by-week plan

This assumes part-time or steady focused work, not a full team with parallel tracks.

### Week 1

* create project structure
* add test harness
* implement canonical model
* implement metadata reader

### Week 2

* build PostgreSQL logical replication reader
* decode and normalize WAL events
* emit to logging sink

### Week 3

* introduce internal Quarkus messaging pipeline
* stabilize event flow
* add more WAL integration tests

### Week 4

* add watermark table support
* add chunk reader
* trigger one-table snapshot

### Week 5

* implement reconciliation engine
* complete overlap/conflict tests

### Week 6

* add checkpoint persistence
* add restart tests
* add minimal REST endpoints

### Week 7+

* add Kafka sink
* harden observability
* improve operational handling

This schedule is intentionally realistic rather than aggressive.

---

## Concrete class creation order

To keep momentum, create classes in the following order.

### First batch

* `TableId`
* `RowKey`
* `SourcePosition`
* `ChangeEnvelope`
* `OperationType`
* `CaptureMode`
* `TableMetadata`
* `PostgresTableMetadataReader`

### Second batch

* `EventSink`
* `LoggingEventSink`
* `PostgresLogicalReplicationClient`
* `PgOutputMessageDecoder`
* `PostgresLogStreamReader`
* `PgOutputEventNormalizer`

### Third batch

* Quarkus channel bridge classes
* pipeline processor classes
* sink wiring classes

### Fourth batch

* `PostgresWatermarkWriter`
* `SnapshotOptions`
* `SnapshotChunkPlan`
* `SnapshotChunkResult`
* `PostgresChunkSqlBuilder`
* `PostgresSnapshotReader`
* `DefaultSnapshotCoordinator`

### Fifth batch

* `WatermarkWindow`
* `WatermarkCoordinator`
* `DefaultWatermarkCoordinator`
* `ReconciliationEngine`
* `DefaultReconciliationEngine`
* `ReconciliationResult`

### Sixth batch

* `CheckpointManager`
* repository classes for checkpoint and snapshot metadata
* REST resources

### Seventh batch

* `KafkaEventSink`
* serializers and topic routing

---

## Development workflow recommendation

Use this loop continuously:

1. write one failing integration or unit test
2. implement the narrowest code to pass it
3. run against real PostgreSQL where behavior matters
4. keep the sink simple until the CDC stage is proven

This project should be test-driven around edge cases, not just API-driven.

---

## Design rules to keep the MVP small

### Rule 1

Every new abstraction must solve a current problem in PostgreSQL MVP work.

### Rule 2

No generalized multi-database design until PostgreSQL snapshot + watermark flow is complete.

### Rule 3

No broker-specific assumptions in the core.

### Rule 4

No advanced concurrency until sequential correctness is proven.

### Rule 5

No more than one active snapshot window per table in MVP.

### Rule 6

Do not build UI or cluster coordination until correctness, restart, and sink boundaries are stable.

---

## High-priority risks and mitigations

### Risk: too much framework code before useful behavior

Mitigation:

* always finish a runnable milestone before adding new architecture layers

### Risk: replication integration consumes all time

Mitigation:

* constrain scope to one table and one publication first

### Risk: reconciliation logic becomes hard to reason about

Mitigation:

* implement it in plain Java with exhaustive unit tests before wiring it deeply

### Risk: internal messaging obscures debugging

Mitigation:

* keep channel graph small and add logging around stage boundaries

### Risk: Kafka is introduced too early and slows progress

Mitigation:

* keep sink pluggable and defer broker work until after reconciliation works

---

## Definition of done for the MVP

The MVP is done when all of the following are true:

* a PostgreSQL table streams live row changes,
* a table snapshot can be triggered,
* watermark-based overlap handling works,
* emitted events use a stable `ChangeEnvelope`,
* a simple sink can be swapped for a Kafka sink without rewriting the core,
* restart behavior is good enough for realistic development and testing.

That is enough to justify moving into the next level of hardening.

---

## Final recommendation

Build this in a narrow, staged way.

Start with:

* PostgreSQL reality,
* plain Java core,
* internal Quarkus messaging,
* simple sink,
* heavy testing around reconciliation.

Then layer in:

* checkpoints,
* REST operations,
* Kafka sink.

This is the shortest route to something real, useful, and extensible without getting trapped in a massive architecture too early.
