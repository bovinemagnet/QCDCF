# Production Hardening Design

**Date:** 2026-03-28
**Status:** Approved
**Author:** Paul Snow

## Context

QCDCF's MVP is feature-complete: WAL capture, watermark reconciliation, snapshot chunking, Kafka sink, REST API, and HTMX dashboard all work. However, the codebase has no retry/reconnection logic, thread safety gaps, resource leaks, and silent error swallowing that make it unsuitable for production workloads. This design addresses all identified issues in a layered bottom-up approach.

## Design Principles

- All new configuration properties have sensible defaults — existing `application.properties` deployments must not break.
- SmallRye Fault Tolerance annotations require CDI proxying, so they can only be placed on CDI beans in `cdc-runtime-quarkus`. Plain Java classes in `cdc-core` and `cdc-postgres` use decorator/wrapper patterns instead.
- All new configuration keys use the `qcdcf` prefix.

## Approach

**Layered Bottom-Up** — fix foundational issues first, then build resilience and observability on top:

1. Thread safety & resource management (foundation)
2. Error propagation & checkpoint atomicity (correctness)
3. Retry & reconnection via SmallRye Fault Tolerance (resilience)
4. Observability & validation (operational)

---

## Layer 1: Thread Safety & Resource Management

### Thread Management

Replace direct `new Thread()` in `ConnectorBootstrap` **and** `ConnectorService` with Quarkus `ManagedExecutor` (injected via CDI).

- Name threads clearly: `qcdcf-wal-reader`, `qcdcf-snapshot-{table}`
- Single WAL reader task submitted to executor
- Snapshot tasks submitted with max 1 concurrent snapshot enforced
- `ConnectorBootstrap.onStop()`: use `executor.shutdown()` + `awaitTermination(10, SECONDS)` instead of raw `Thread.interrupt()`
- Fix status lifecycle: set `RUNNING` only after WAL connection is confirmed, not immediately after thread start

### Volatile/Visibility Fixes

**PostgresLogStreamReader** — make shared fields volatile:
- `lastProcessedLsn` (read by health checks from different thread)
- `currentTxId`
- `lastCommitTimestamp`

**ConnectorService** — replace individual volatile fields with `AtomicReference<SnapshotState>` record:
```java
record SnapshotState(String table, String status, long rowCount) {}
private final AtomicReference<SnapshotState> snapshotState =
    new AtomicReference<>(new SnapshotState(null, "NONE", 0));
```

**WatermarkAwareEventRouter** — document single-thread contract in Javadoc and add thread assertion in debug builds. This component is designed to be called from the WAL reader thread only.

### Resource Lifecycle

- `KafkaEventSink`: implement `AutoCloseable`, call `producer.close(Duration.ofSeconds(5))` in `close()`
- `EventSinkProducer`: call `close()` on the produced sink via `@PreDestroy`
- `EventSink` interface: add `extends AutoCloseable` with a default no-op `close()` method to avoid breaking existing implementations (`InMemoryEventSink`, `LoggingEventSink`, test doubles in `cdc-test-kit`)

### Files Modified
- `cdc-runtime-quarkus/.../bootstrap/ConnectorBootstrap.java`
- `cdc-runtime-quarkus/.../service/ConnectorService.java`
- `cdc-runtime-quarkus/.../kafka/KafkaEventSink.java`
- `cdc-runtime-quarkus/.../service/EventSinkProducer.java`
- `cdc-postgres/.../replication/PostgresLogStreamReader.java`
- `cdc-core/.../sink/EventSink.java`
- `cdc-core/.../watermark/WatermarkAwareEventRouter.java`

---

## Layer 2: Error Propagation & Checkpoint Atomicity

### Error Propagation

Replace fire-and-forget `WalIngressChannelBridge.emit()` with synchronous publish in `PostgresLogStreamReader`.

- Reader calls `sink.publish(envelope)` directly and checks `PublishResult`
- On `PublishResult.Failure`: do NOT advance `lastProcessedLsn`, do NOT acknowledge LSN
- Event will be retried on next iteration (Layer 3 adds structured retry)
- This ensures the WAL reader never advances past an event it failed to deliver
- **Migration note:** audit for any `@Incoming("wal-normalised")` consumers before removing `WalIngressChannelBridge`. Currently only `PublishRequestProcessor` consumes this channel — it will be replaced by the direct sink call.

### Checkpoint-LSN Atomicity (Bug Fix)

**Current bug:** In `PostgresLogStreamReader.handleRawMessage()` (lines 131-134), LSN acknowledgement happens **before** `saveCheckpoint()`. If checkpoint save fails after LSN ack, on restart PostgreSQL will not replay those events — data loss.

**Fix:** Combine checkpoint save and LSN acknowledgement into a single `commitProgress()` method with reversed order:

1. Save checkpoint to database **first**
2. Acknowledge LSN to PostgreSQL **second**

**Invariant:** checkpoint-first. If checkpoint save fails, LSN is not acknowledged. On restart, PostgreSQL replays from last acknowledged LSN (safe, at-least-once).

### Watermark Window Cleanup

Wrap chunk-read + close-window in `DefaultSnapshotCoordinator` with try-finally. Note: `closeWindow()` takes a `WatermarkWindow` parameter in the current API.

```java
WatermarkWindow window = watermarkCoordinator.openWindow(...);
try {
    ChunkReadResult result = chunkReader.read(plan);
    WatermarkWindow closed = watermarkCoordinator.closeWindow(window);
    chunkHandler.onChunk(enrichedEvents, result);
} catch (Exception e) {
    watermarkCoordinator.cancelWindow(window);  // NEW method
    throw e;
}
```

Add `cancelWindow(WatermarkWindow)` to `WatermarkCoordinator` interface: clears buffered events, resets window state, logs warning.

### Files Modified
- `cdc-postgres/.../replication/PostgresLogStreamReader.java`
- `cdc-core/.../snapshot/DefaultSnapshotCoordinator.java`
- `cdc-core/.../watermark/WatermarkCoordinator.java`
- `cdc-core/.../watermark/WatermarkAwareEventRouter.java`
- `cdc-runtime-quarkus/.../messaging/WalIngressChannelBridge.java` (removed)
- `cdc-runtime-quarkus/.../messaging/PublishRequestProcessor.java` (removed)

---

## Layer 3: Retry & Reconnection (SmallRye Fault Tolerance)

### New Dependencies

Add to `cdc-runtime-quarkus/build.gradle`:
- `quarkus-smallrye-fault-tolerance`
- `quarkus-micrometer-registry-prometheus` (if not already present — needed for fault tolerance metrics)

### CDI Constraint

SmallRye Fault Tolerance annotations require CDI proxying. They can **only** be placed on CDI-managed beans in `cdc-runtime-quarkus`. For plain Java classes in `cdc-core` and `cdc-postgres`, we use the decorator pattern (e.g., `RetryingEventSink`, `ResilientCheckpointRepository`).

### WAL Reader Reconnection

`ConnectorBootstrap` (a CDI bean) wraps WAL reader start in a retry method:

- **@Retry**: maxRetries = -1 (unlimited in SmallRye), delay = 1s, jitter = 200ms, retryOn = {SourceReadException.class}
- Exponential backoff: factor = 2, maxDelay = 60s
- On each retry: log WARN with attempt count and last error
- **Sustained failure detection:** instead of `@Fallback` (which won't trigger with unlimited retries), use a health check that tracks `lastSuccessfulConnection` timestamp. If `now - lastSuccessfulConnection > configurable threshold` (default 10 minutes), `SourceHealthCheck` reports DOWN.

### Snapshot Chunk Retry

`ConnectorService` (a CDI bean) uses `@Retry` on a chunk-execution method:

- maxRetries = 3, delay = 2s, exponential backoff
- Failed chunk retries from same primary key position (idempotent)
- After max retries exhausted: mark snapshot FAILED, log error, continue WAL streaming

### Sink Publish Retry

Create `RetryingEventSink` CDI decorator wrapping any `EventSink`:

- `@Retry` on `publish()`: maxRetries = 3, delay = 500ms, backoff factor = 2
- `@Timeout(5000)` on `publish()` — prevents indefinite blocking on Kafka `.get()`
- After max retries: return `PublishResult.Failure` (WAL reader handles per Layer 2)

### Circuit Breaker for Checkpoint

Create `ResilientCheckpointRepository` CDI wrapper around `PostgresCheckpointRepository`:

- **@CircuitBreaker** on `save()`: open after 5 failures in 10s window, half-open after 30s
- Prevents hammering a failing database

### Resilience Metrics

SmallRye Fault Tolerance auto-publishes metrics via Micrometer when both extensions are present:
- `ft.retry.calls.total` (tagged by method)
- `ft.circuitbreaker.state.total`
- `ft.timeout.calls.total`

Additionally, add custom QCDCF metrics:
- `qcdcf_wal_reconnect_attempts_total` (counter)
- `qcdcf_sink_publish_retries_total` (counter)
- `qcdcf_snapshot_chunk_retries_total` (counter)

### Configuration Properties

```properties
# WAL reader reconnection
qcdcf.resilience.wal-retry.delay=1s
qcdcf.resilience.wal-retry.max-delay=60s
qcdcf.resilience.wal-retry.jitter=200ms

# Snapshot chunk retry
qcdcf.resilience.snapshot-retry.max-retries=3
qcdcf.resilience.snapshot-retry.delay=2s

# Sink publish retry
qcdcf.resilience.sink-retry.max-retries=3
qcdcf.resilience.sink-retry.delay=500ms
qcdcf.resilience.sink-retry.timeout=5s

# Checkpoint circuit breaker
qcdcf.resilience.checkpoint-cb.failure-threshold=5
qcdcf.resilience.checkpoint-cb.window=10s
qcdcf.resilience.checkpoint-cb.half-open-delay=30s

# Kafka producer
qcdcf.kafka.request-timeout-ms=30000
qcdcf.kafka.delivery-timeout-ms=120000
```

### Files Modified
- `cdc-runtime-quarkus/build.gradle` (new dependencies)
- `cdc-runtime-quarkus/.../bootstrap/ConnectorBootstrap.java`
- `cdc-runtime-quarkus/.../service/ConnectorService.java`
- `cdc-runtime-quarkus/.../service/EventSinkProducer.java` (RetryingEventSink wiring)
- `cdc-runtime-quarkus/.../service/RetryingEventSink.java` (new)
- `cdc-runtime-quarkus/.../service/ResilientCheckpointRepository.java` (new)
- `cdc-runtime-quarkus/.../config/ConnectorRuntimeConfig.java`

---

## Layer 4: Observability & Validation

### Startup Configuration Validation

Add a `@PostConstruct` method to `ConnectorBootstrap` running pre-flight checks before WAL reader start:

- Validate: DB connectivity, `wal_level=logical`, replication slot exists, publication exists, watermark table exists
- Extract shared logic from `CheckCommand` into `ConnectorValidator` CDI service (also fix SQL injection — use parameterised queries instead of string concatenation)
- On failure: log ERROR with actionable hints (reuse `diagnoseFailure()`), set status to FAILED, do NOT start reader
- Configurable: `qcdcf.connector.validate-on-startup=true` (default)

### Health Check Improvements

- **LivenessHealthCheck**: verify WAL reader executor hasn't terminated unexpectedly
- **SourceHealthCheck**: add WAL lag metric — if lag > configurable threshold (default 60s), report DOWN. Also check `lastSuccessfulConnection` timestamp for sustained-failure detection (Layer 3).
- **New SinkHealthCheck**: verify sink reachable (Kafka broker connectivity)

### Bounded Buffers

`WatermarkAwareEventRouter.bufferedLogEvents`: configurable max size (default 100,000).

When buffer exceeds limit: cancel current watermark window, log ERROR, retry chunk with smaller chunk size. Prevents OOM during high-throughput snapshot windows.

Configuration: `qcdcf.snapshot.max-buffer-size=100000`

### Dashboard Fixes

Replace swallowed exceptions in `DashboardResource`:

- `buildTablesData()`: catch, log with context, return empty list with "database unavailable" indicator
- `countRows()`: same pattern — also fix SQL injection by using parameterised queries
- No more empty catch blocks

### Configuration Properties (Layer 4)

```properties
# Startup validation
qcdcf.connector.validate-on-startup=true

# Health thresholds
qcdcf.health.wal-lag-threshold=60s
qcdcf.health.sustained-failure-threshold=10m

# Buffer limits
qcdcf.snapshot.max-buffer-size=100000
```

### Files Modified
- `cdc-runtime-quarkus/.../bootstrap/ConnectorBootstrap.java`
- `cdc-runtime-quarkus/.../health/LivenessHealthCheck.java`
- `cdc-runtime-quarkus/.../health/SourceHealthCheck.java`
- `cdc-runtime-quarkus/.../health/SinkHealthCheck.java` (new)
- `cdc-runtime-quarkus/.../service/ConnectorValidator.java` (new, extracted from CheckCommand)
- `cdc-runtime-quarkus/.../cli/CheckCommand.java` (delegates to ConnectorValidator)
- `cdc-runtime-quarkus/.../dashboard/DashboardResource.java`
- `cdc-runtime-quarkus/.../service/EventSinkProducer.java`
- `cdc-core/.../watermark/WatermarkAwareEventRouter.java`
- `cdc-runtime-quarkus/.../config/ConnectorRuntimeConfig.java`

---

## Testing Strategy

Each layer has its own test additions:

- **Layer 1**: Unit tests for `SnapshotState` atomicity, verify `KafkaEventSink.close()` is called via mock/spy, verify `ManagedExecutor` shutdown sequence
- **Layer 2**: TDD tests for `commitProgress()` ordering (checkpoint before LSN ack), `cancelWindow()` behaviour on exception, error propagation from sink failure back to reader (verify LSN not advanced)
- **Layer 3**: Use Mockito spies to simulate transient failures and verify retry behaviour. Test `RetryingEventSink` with mock that fails N times then succeeds. Test `ResilientCheckpointRepository` circuit breaker state transitions. For WAL reconnection, mock the `PostgresLogicalReplicationClient` to throw `SourceReadException` then succeed.
- **Layer 4**: Unit test for `ConnectorValidator` with mocked DataSource (pass/fail scenarios), health check tests with mock executor state and timestamp tracking

## Verification

- `gradle test` — all existing tests continue to pass
- `gradle :cdc-core:test` — new reconciliation and watermark tests pass
- `gradle :cdc-runtime-quarkus:test` — new retry, health check, and validation tests pass
- `gradle antora` — documentation builds without errors
- Manual: start with misconfigured slot name, verify startup validation catches it and logs actionable error
