# PRD: Quarkus CDC Framework Inspired by Netflix DBLog

## 1. Document status

**Status:** Draft v1
**Audience:** Engineering, architecture, platform, SRE
**Author:** Product/Architecture draft generated from the public DBLog description
**Target implementation:** Java 21+, Gradle, Quarkus

## 2. Product summary

Build a reusable CDC framework for relational databases that:

* streams committed row changes with low latency from the database transaction log,
* captures full table state through chunked table reads,
* allows snapshotting to run **without pausing log streaming for long periods**,
* reconciles snapshot rows and log events using a **watermark-based coordination model**,
* supports full snapshot, per-table snapshot, and targeted primary-key snapshot,
* publishes canonical change events to Kafka or a similar downstream bus. ([Netflix Tech Blog][1])

## 3. Background and problem statement

Traditional CDC approaches tend to split into two weak options:

* **log-only CDC**, which is great for new changes but cannot reconstruct full current state when transaction log retention is limited;
* **snapshot-based extraction**, which can capture the state but often interferes with ongoing writes, introduces ordering issues, or requires stalling event delivery. ([arXiv][3])

The Netflix DBLog idea solves that by combining:

* transaction-log event streaming for freshness, and
* chunked table reads for state capture,

with a watermark mechanism that lets those two streams be merged safely while log processing keeps moving. The paper explicitly says selects are chunked, progress is tracked, the approach avoids locks, and it can be triggered for all tables, one table, or selected keys. ([arXiv][3])

## 4. Vision

Create an internal platform capability that application teams can use to:

* replicate database tables into Kafka,
* bootstrap or re-bootstrap datasets without downtime,
* rehydrate downstream search indexes, caches, data lakes, and projections,
* support selective backfills by table or key range,
* do all of this with low operational friction.

## 5. Product goals

### Primary goals

1. **Low-latency CDC**
   Capture committed changes and publish them downstream quickly after commit. This is a direct match to DBLog’s purpose of keeping targets closely in sync. ([arXiv][3]) Where postgres is the primary target for initial release.

2. **Online state capture**
   Capture the current state of a table while ongoing writes continue.

3. **No table locks**
   Avoid long blocking locks or pauses on source write traffic. The DBLog paper explicitly calls out a watermark approach with minimum source impact and no locks. ([arXiv][3])

4. **Chunked, resumable snapshots**
   Read large tables in chunks, store progress, and resume after failure. ([arXiv][3])

5. **Generic framework**
   Provide a core engine with pluggable adapters for different source databases and downstream sinks.

6. **Operational safety**
   Make backpressure, retries, idempotency, lag visibility, and recovery first-class concerns.

7. **Test Driven Development**
   Make sure that test are written first, before code is delivered.

### Secondary goals

* observability and traceability,
* schema evolution handling,
* multi-tenant connector/runtime model,
* selective snapshots by table or primary key,
* local developer experience using Quarkus Dev Services for Kafka. ([Quarkus][4])

## 6. Non-goals

For v1, this product will **not** aim to be:

* a full competitor to Debezium’s entire connector ecosystem,
* a universal DDL migration platform,
* a bi-directional replication engine,
* a data transformation engine with arbitrary joins/aggregations,
* a GUI-heavy SaaS product.

This should start as a **runtime + API + admin model**, not an overbuilt control plane.

## 7. Target users

### Primary users

* platform engineers,
* integration teams,
* application teams needing CDC feeds,
* search/indexing teams,
* analytics ingestion teams.

### Secondary users

* SRE/operations teams,
* security/compliance teams,
* downstream consumers of domain events.

## 8. Key use cases

1. **Initial table bootstrap**
   Publish all rows of selected tables without stopping low-latency log streaming. ([arXiv][3])

2. **Continuous replication**
   Mirror row-level changes to Kafka topics.

3. **Partial backfill**
   Re-snapshot one table after downstream corruption.

4. **Targeted re-sync**
   Re-snapshot a set of primary keys for correction workflows. The DBLog paper explicitly mentions triggering selects for specific primary keys. ([arXiv][3])

5. **State rebuild**
   Rebuild Elasticsearch/OpenSearch indexes or read models from a CDC stream plus replayable topics.

6. **Tenant onboarding**
   Bootstrap a newly onboarded tenant’s dataset while streaming changes.

## 9. Product principles

1. **Correctness over cleverness**
   Snapshot/log reconciliation must be explicit and testable.

2. **Generic core, specific adapters**
   The watermark/chunk engine belongs in the core; WAL/binlog parsing belongs in adapters.

3. **Durable progress**
   Every unit of work must be restartable from durable metadata.

4. **Backpressure-aware**
   Slow sinks must not silently corrupt ordering or memory usage.

5. **Operationally boring**
   Reasonable defaults, clear health endpoints, good metrics, obvious runbooks.

## 10. Functional requirements

## 10.1 Source database support

v1 should support one database only. I recommend:

* **PostgreSQL first**, because it has strong CDC options and fits well with modern eventing patterns.

Later:

* MySQL
* Oracle
* SQL Server

The framework should expose a `SourceAdapter` SPI with capabilities such as:

* stream log changes,
* write/read watermark markers,
* perform chunked table scans,
* resolve table metadata and primary keys,
* serialize source positions.

## 10.2 Change event capture

The system must:

* read committed row changes from the source log,
* normalize them into a canonical internal event model,
* include operation type: `INSERT`, `UPDATE`, `DELETE`, optionally `SNAPSHOT`,
* preserve source ordering metadata as much as possible,
* attach transaction and source-position metadata.

Canonical event envelope:

```json
{
  "eventId": "uuid",
  "source": {
    "database": "appdb",
    "schema": "public",
    "table": "customer",
    "position": "lsn/binlog-pos/etc",
    "transactionId": "optional",
    "commitTimestamp": "2026-03-19T10:15:30Z"
  },
  "key": {
    "customer_id": 123
  },
  "operation": "UPDATE",
  "payload": {
    "before": { },
    "after": { }
  },
  "captureMode": "LOG",
  "watermarkContext": null
}
```

## 10.3 Snapshot/state capture

The system must support:

* full-database snapshot,
* per-table snapshot,
* targeted key snapshot,
* resumable chunk progress,
* configurable chunk size,
* configurable ordering by primary key,
* snapshot cancellation and restart. ([arXiv][3])

Each chunk should:

* define a bounded key range or bounded row-count window,
* emit rows as `SNAPSHOT` events,
* participate in watermark reconciliation before being considered complete.

## 10.4 Watermark coordination

This is the heart of the product.

The engine must support a watermark protocol broadly equivalent to the DBLog design:

1. log reader streams changes normally,
2. before a chunk read, insert or emit a **low watermark** marker,
3. read the chunk from the table,
4. insert or emit a **high watermark** marker,
5. reconcile the snapshot rows against log events observed between those markers,
6. emit the final merged stream in a deterministic way. ([Netflix Tech Blog][1])

The exact internal algorithm can differ from Netflix’s implementation, but the product must guarantee:

* no missing rows during concurrent updates,
* no stale snapshot row overwriting a newer log event,
* no double-publication of the same logical row version without explicit dedupe semantics.

### Required watermark semantics

* watermark markers must be durable and tied to source position,
* chunk reconciliation state must survive restart,
* late-arriving or buffered log events inside the watermark window must be processed deterministically,
* chunk completion must be idempotent.

## 10.5 Output transport

v1 sink: **Kafka**

The system must:

* publish change events to a topic per table or topic per domain,
* support configurable partition key,
* support at-least-once delivery,
* expose hooks for downstream idempotency,
* optionally publish control events to separate topics.

Quarkus Messaging supports Kafka integration through its messaging extensions and can also use Kafka clients directly. ([Quarkus][5])

## 10.6 Control plane/API

Provide REST endpoints or admin commands for:

* register connector,
* start connector,
* pause connector,
* resume connector,
* stop connector,
* trigger full snapshot,
* trigger table snapshot,
* trigger key-based re-snapshot,
* view lag and health,
* view current chunk progress,
* inspect watermark windows,
* replay failed publications.

Suggested REST paths:

* `POST /connectors`
* `POST /connectors/{id}/start`
* `POST /connectors/{id}/pause`
* `POST /connectors/{id}/snapshots`
* `GET /connectors/{id}/status`
* `GET /connectors/{id}/metrics`
* `GET /connectors/{id}/tables/{table}/progress`

## 10.7 Progress tracking and metadata

Persist operational metadata in a framework-owned metadata schema.

Required tables:

* `cdc_connector`
* `cdc_connector_state`
* `cdc_table_config`
* `cdc_snapshot_job`
* `cdc_snapshot_chunk`
* `cdc_watermark_window`
* `cdc_publication_checkpoint`
* `cdc_dead_letter_event`

These store:

* connector config,
* source offset/checkpoint,
* current chunk boundaries,
* watermark markers,
* reconciliation status,
* publication retries,
* dead-letter state.

## 10.8 Error handling

The system must classify failures into:

* source read failures,
* metadata persistence failures,
* reconciliation failures,
* sink publication failures,
* schema mismatch failures,
* poison message failures.

Expected behavior:

* retry transient failures with backoff,
* circuit-break on repeated source/sink instability,
* do not advance durable checkpoint until publication contract is met,
* provide DLQ or quarantine path for unprocessable records.

## 10.9 Observability

The platform must expose:

* connector health,
* source lag,
* publish lag,
* snapshot progress,
* rows/sec by table,
* chunk duration,
* reconciliation buffer size,
* retry counts,
* error rates,
* watermark window depth,
* Kafka publication latency.

Quarkus provides health, metrics, and OpenTelemetry support; tracing is especially useful for following a chunk from trigger to publication. ([Quarkus][6])

## 10.10 Scheduling

Snapshot jobs may run:

* on-demand,
* scheduled,
* or resumed after interruption.

For a clustered runtime, Quartz is a good fit because Quarkus’s Quartz extension is specifically for clustered periodic tasks, whereas the simpler scheduler is for in-memory scheduling. ([Quarkus][7])

My recommendation:

* use **on-demand admin-triggered snapshots** in MVP,
* add Quartz only when you need clustered scheduled backfills.

## 11. Non-functional requirements

## 11.1 Performance

MVP targets:

* log event p95 publish latency under 2 seconds in steady state,
* snapshot chunk throughput configurable by table,
* bounded memory for reconciliation buffers,
* ability to process at least one large table without full in-memory materialization.

## 11.2 Reliability

* restart-safe at every stage,
* no data loss under process crash after committed source read but before sink ack,
* idempotent replay from checkpoints,
* reconciliation state persisted durably.

## 11.3 Scalability

* multiple connectors per runtime,
* horizontal scaling by connector assignment,
* separate worker pools for log reading, snapshot reading, and sink publishing,
* bounded concurrency to protect source DB.

## 11.4 Security

* least-privilege DB access,
* secrets from external config/secrets manager,
* TLS to DB and Kafka,
* audit trail for admin-triggered snapshots,
* topic-level ACL support.

## 11.5 Operability

* one-command local dev,
* container deployment,
* Kubernetes-friendly health probes,
* clear dashboards and alert thresholds.

Quarkus’s Gradle tooling and Dev Services improve local setup, especially for Kafka-backed development. ([Quarkus][2])

## 12. MVP scope

## 12.1 In scope for MVP

* PostgreSQL source adapter
* Kafka sink
* full-table and per-table snapshots
* watermark-based chunk reconciliation
* resumable checkpoints
* REST admin API
* metadata persistence
* Prometheus metrics + health
* local Docker/Testcontainers-based integration tests
* Gradle multi-module build
* Quarkus runtime

## 12.2 Out of scope for MVP

* multi-DB support
* UI console
* schema registry integration beyond optional hooks
* automatic DDL evolution
* cross-region failover
* exactly-once end-to-end semantics
* non-Kafka sinks

## 13. Success metrics

### Product metrics

* time to onboard a new table under 30 minutes,
* successful initial bootstrap of a 100M+ row table without service write outage,
* zero unexplained row mismatches in reconciliation tests,
* less than 1 hour MTTR for common connector failures.

### Technical metrics

* p95 log-to-Kafka latency,
* snapshot completion time by table size,
* percentage of resumptions completed without manual intervention,
* source DB overhead during snapshot under agreed threshold.

## 14. User stories

### Platform engineer

As a platform engineer, I want to register a PostgreSQL connector and start streaming changes from selected tables into Kafka so that downstream systems can subscribe without custom polling.

### Application team

As a service team, I want to trigger a re-snapshot of one table after downstream corruption so that I can restore consistency without full rebootstrap.

### Operations

As an operator, I want to see lag, failed chunks, and current watermark windows so that I can diagnose replication delays quickly.

### Data consumer

As a consumer, I want a stable event envelope and topic naming convention so that I can build replayable consumers.

## 15. Proposed architecture for Gradle + Quarkus

This is where I would adapt the DBLog concept to your stack.

## 15.1 Module structure

```text
cdc-platform/
  build.gradle
  settings.gradle

  cdc-core/
    watermark engine
    reconciliation model
    event envelope
    checkpoints
    SPI interfaces

  cdc-source-spi/
    SourceAdapter
    LogReader
    SnapshotReader
    WatermarkStore abstractions

  cdc-source-postgres/
    PostgreSQL implementation
    logical replication / WAL integration
    chunked snapshot SQL generator

  cdc-sink-spi/
    SinkPublisher
    delivery result contracts

  cdc-sink-kafka/
    Kafka publisher
    topic resolver
    serializer

  cdc-metadata/
    metadata entities / repositories
    Flyway or Liquibase migrations

  cdc-runtime-quarkus/
    Quarkus app
    REST API
    health
    metrics
    config
    startup orchestration

  cdc-test-kit/
    contract tests
    test fixtures
    Testcontainers helpers
```

## 15.2 Quarkus extension choices

Recommended Quarkus pieces:

* `quarkus-rest` or RESTEasy Reactive for admin APIs
* `quarkus-jdbc-postgresql` for metadata DB and possibly source helper queries
* `quarkus-messaging-kafka` or Kafka client integration for sink publishing
* `quarkus-smallrye-health`
* `quarkus-micrometer` or OTel metrics stack
* `quarkus-opentelemetry`
* `quarkus-quartz` only if you need clustered scheduling
* `flyway` or `liquibase` for metadata schema migrations

Quarkus provides unified datasource configuration, Kafka messaging, Gradle support, and clustered scheduling via Quartz. ([Quarkus][8])

## 15.3 Runtime components

### ConnectorSupervisor

Owns lifecycle of connectors and worker assignment.

### LogIngestWorker

Reads source log changes and converts them to canonical events.

### SnapshotCoordinator

Creates snapshot jobs and chunk plans.

### SnapshotChunkWorker

Executes chunk SQL reads and passes rows to reconciliation.

### WatermarkCoordinator

Creates low/high watermark boundaries and manages buffering rules.

### ReconciliationEngine

Merges log events and snapshot rows deterministically.

### PublicationWorker

Publishes merged events to Kafka and updates checkpoints.

### MetadataStore

Persists connector state, chunk progress, windows, and retries.

## 16. Core data model

### Canonical row event

```java
public record RowChangeEvent(
    UUID eventId,
    SourceRef source,
    Map<String, Object> key,
    Operation operation,
    CaptureMode captureMode,
    Map<String, Object> before,
    Map<String, Object> after,
    Instant observedAt,
    WatermarkContext watermarkContext
) {}
```

### Snapshot chunk descriptor

```java
public record SnapshotChunk(
    UUID jobId,
    String tableName,
    String pkStartInclusive,
    String pkEndExclusive,
    int chunkSize,
    ChunkStatus status
) {}
```

### Watermark window

```java
public record WatermarkWindow(
    UUID windowId,
    String tableName,
    String lowWatermark,
    String highWatermark,
    WindowStatus status
) {}
```

## 17. Reconciliation rules

The product must define explicit behavior for rows affected during snapshot windows.

Suggested rules:

1. If a row appears in snapshot chunk and **no newer log event** exists in the watermark window, emit the snapshot row.
2. If a newer log event exists for that key within the window, prefer the log event.
3. If a row is deleted after the chunk select but before publication, emit delete semantics based on log precedence.
4. If multiple log updates occur for the same key within the window, keep source-order semantics and emit the final valid sequence.
5. If a row is updated before snapshot select and again after high watermark, the checkpoint boundary determines whether later events are already covered by normal log processing.

This is the hard part. I would make this area the centerpiece of the design and test strategy.

## 18. API requirements

## 18.1 Admin API examples

### Create connector

```http
POST /connectors
Content-Type: application/json

{
  "name": "customer-db",
  "sourceType": "postgres",
  "metadataDatasource": "default",
  "sourceConfig": {
    "jdbcUrl": "jdbc:postgresql://...",
    "publication": "cdc_pub",
    "slotName": "cdc_slot"
  },
  "sinkConfig": {
    "type": "kafka",
    "bootstrapServers": "...",
    "topicPrefix": "cdc."
  }
}
```

### Trigger table snapshot

```http
POST /connectors/customer-db/snapshots
Content-Type: application/json

{
  "mode": "TABLE",
  "tables": ["public.customer", "public.account"],
  "chunkSize": 10000
}
```

### Trigger targeted key re-sync

```http
POST /connectors/customer-db/snapshots
Content-Type: application/json

{
  "mode": "KEYS",
  "table": "public.customer",
  "keys": [
    {"customer_id": 101},
    {"customer_id": 205}
  ]
}
```

## 19. Delivery semantics

For MVP, state the contract honestly:

* **at-least-once delivery** to Kafka,
* ordering guaranteed per partition key, not globally,
* downstream consumers must support idempotency,
* replay from checkpoints supported.

Do not promise exactly-once in v1 unless you are willing to pay for the complexity all the way through metadata store, source offsets, sink transactions, and restart semantics.

## 20. Risks

### 1. Watermark correctness bugs

Most likely failure mode. Requires deep test coverage.

### 2. Source DB impact

Poor chunk sizing or bad PK scans can hurt OLTP performance.

### 3. WAL/binlog adapter complexity

The source-specific layer will dominate effort.

### 4. Large-row serialization costs

Wide tables can create CPU and network pressure.

### 5. Schema drift

Column adds/removes and type changes can break consumers.

### 6. Misleading “generic” abstraction

Too much abstraction too early can make the first source adapter worse.

My recommendation: keep the generic API narrow and prove it with PostgreSQL first.

## 21. Testing strategy

This should be much stronger than a normal CRUD app.

### 21.1 Unit tests

* watermark window state machine,
* chunk planning,
* reconciliation rules,
* checkpoint serialization,
* retry policies.

### 21.2 Integration tests

Use Testcontainers with PostgreSQL and Kafka.

Test scenarios:

* insert/update/delete during active chunk read,
* repeated updates to same key in watermark window,
* delete-after-snapshot-before-high-watermark,
* connector crash after chunk read but before publish,
* connector crash after publish but before checkpoint,
* partial table snapshot resume,
* long-running snapshot while live writes continue.

### 21.3 Deterministic simulation tests

Build a simulation harness that:

* generates source mutations,
* schedules chunk boundaries,
* injects failures,
* compares final emitted state to expected relational truth.

### 21.4 Performance tests

* large table scans,
* hot-table write load during snapshot,
* lag buildup under slow Kafka,
* memory behavior of reconciliation buffers.

## 22. Rollout plan

### Phase 1: Technical spike

* prove watermark algorithm for PostgreSQL on one table,
* emit to Kafka,
* validate with conflict-heavy test cases.

### Phase 2: Internal MVP

* one runtime,
* metadata persistence,
* admin endpoints,
* observability,
* two or three pilot tables.

### Phase 3: Hardened platform

* resumability,
* runbooks,
* alerting,
* topic conventions,
* consumer onboarding docs.

### Phase 4: Generalization

* multi-table orchestration,
* selective key resync,
* second source adapter if justified.

## 23. Open questions

1. Will the source adapter read native logical replication, or will it layer on an existing CDC library?
2. Will watermark markers be written into a dedicated source table, or represented only through source-position mechanics?
3. What is the exact sink contract: raw row CDC, envelope + metadata, or domain-normalized events?
4. How much schema evolution support is required in v1?
5. Is metadata stored in the source DB, separate operational DB, or both?
6. Do we want topic-per-table or topic-per-bounded-context?
7. Is reordering across snapshot/log boundaries acceptable if per-key correctness is preserved?

## 24. Recommended technical decisions

Here’s the opinionated version I would choose for your stack:

### Use PostgreSQL first

Do not start with a “generic database” ambition. Build the generic framework around one real adapter.

### Use Quarkus for runtime, not for core logic

Put the watermark engine in plain Java modules. Keep Quarkus in the runtime/API/observability layer.

### Use Kafka as the first-class sink

It gives you durable replay and cleaner downstream fan-out. Quarkus has native support for Kafka messaging. ([Quarkus][5])

### Use Quartz only when clustering needs it

For a single-node internal tool, a simpler trigger model is enough. Use Quartz when you need clustered scheduled execution. ([Quarkus][7])

### Start with at-least-once

Be explicit. Make dedupe practical. Don’t chase exactly-once before the platform is real.

### Build a brutal reconciliation test suite

That is the product. The REST API and admin model are secondary.

## 25. Suggested Gradle build direction

Quarkus supports Gradle project generation, Gradle wrapper installation, dev mode through `quarkusDev`, extension management via Gradle tasks, and normal build packaging through Gradle. ([Quarkus][2])

A practical build direction:

```groovy
plugins {
    id 'java'
    id 'io.quarkus' apply false
}

subprojects {
    apply plugin: 'java'

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    repositories {
        mavenCentral()
    }

    test {
        useJUnitPlatform()
    }
}
```

And in the runtime module:

```groovy
plugins {
    id 'java'
    id 'io.quarkus'
}

dependencies {
    implementation enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}")
    implementation 'io.quarkus:quarkus-rest'
    implementation 'io.quarkus:quarkus-jdbc-postgresql'
    implementation 'io.quarkus:quarkus-messaging-kafka'
    implementation 'io.quarkus:quarkus-smallrye-health'
    implementation 'io.quarkus:quarkus-opentelemetry'
    implementation 'io.quarkus:quarkus-quartz'
}
```

## 26. Executive summary

The DBLog-inspired product you want to build is:

* a **CDC + online snapshot framework**,
* centered on **watermark-based coordination** between live log events and chunked table reads,
* best implemented in your stack as a **plain Java core engine** with a **Quarkus runtime shell**,
* and should start as a **PostgreSQL + Kafka + Quarkus + Gradle** platform, not a universal CDC product.

That gives you the highest chance of actually finishing it.

---

If you want, I can turn this into a second document: a **technical architecture spec** with module-by-module classes, interfaces, sequence diagrams, and a suggested PostgreSQL implementation approach.

[1]: https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b?utm_source=chatgpt.com "DBLog: A Generic Change-Data-Capture Framework"
[2]: https://quarkus.io/guides/gradle-tooling "Quarkus and Gradle - Quarkus"
[3]: https://arxiv.org/abs/2010.12597 "[2010.12597] DBLog: A Watermark Based Change-Data-Capture Framework"
[4]: https://quarkus.io/guides/kafka-dev-services "Dev Services for Kafka - Quarkus"
[5]: https://quarkus.io/guides/kafka-getting-started "Getting Started to Quarkus Messaging with Apache Kafka - Quarkus"
[6]: https://quarkus.io/guides/opentelemetry-tracing "Using OpenTelemetry Tracing - Quarkus"
[7]: https://quarkus.io/guides/quartz "Scheduling Periodic Tasks with Quartz - Quarkus"
[8]: https://quarkus.io/guides/datasource "Configure data sources in Quarkus - Quarkus"
