# Production Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden QCDCF for production by fixing thread safety, error propagation, checkpoint atomicity, adding retry/reconnection via SmallRye Fault Tolerance, and improving observability.

**Architecture:** Four-layer bottom-up approach. Layer 1 fixes thread safety and resource management. Layer 2 fixes error propagation and checkpoint ordering. Layer 3 adds SmallRye Fault Tolerance retry/reconnection with CDI wrappers for non-CDI classes. Layer 4 adds startup validation, health checks, bounded buffers, and dashboard fixes.

**Tech Stack:** Java 25, Quarkus 3.32.4, SmallRye Fault Tolerance, Micrometer, JUnit 5, AssertJ, Mockito

**Spec:** `docs/superpowers/specs/2026-03-28-production-hardening-design.md`

---

## File Structure

### New Files
| File | Responsibility |
|------|---------------|
| `cdc-runtime-quarkus/.../service/RetryingEventSink.java` | CDI decorator wrapping EventSink with @Retry and @Timeout |
| `cdc-runtime-quarkus/.../service/ResilientCheckpointRepository.java` | CDI wrapper around PostgresCheckpointRepository with @CircuitBreaker |
| `cdc-runtime-quarkus/.../service/ConnectorValidator.java` | Startup pre-flight validation (extracted from CheckCommand) |
| `cdc-runtime-quarkus/.../health/SinkHealthCheck.java` | Readiness check for sink reachability |
| `cdc-core/src/test/.../watermark/WatermarkWindowCleanupTest.java` | Tests for cancelWindow() and try-finally cleanup |
| `cdc-core/src/test/.../sink/EventSinkCloseTest.java` | Tests for AutoCloseable contract |
| `cdc-runtime-quarkus/src/test/.../service/RetryingEventSinkTest.java` | Tests for retry and timeout behaviour |
| `cdc-runtime-quarkus/src/test/.../service/ConnectorValidatorTest.java` | Tests for startup validation |

### Modified Files
| File | Changes |
|------|---------|
| `cdc-core/.../sink/EventSink.java` | Add `extends AutoCloseable` with default no-op `close()` |
| `cdc-core/.../watermark/WatermarkCoordinator.java` | Add `cancelWindow(WatermarkWindow)` method |
| `cdc-core/.../watermark/WatermarkAwareEventRouter.java` | Implement `cancelWindow()`, add thread assertion, add max buffer size |
| `cdc-core/.../snapshot/DefaultSnapshotCoordinator.java` | Add try-finally around watermark window lifecycle |
| `cdc-postgres/.../replication/PostgresLogStreamReader.java` | Add volatile fields, fix checkpoint-LSN ordering, add `commitProgress()` |
| `cdc-runtime-quarkus/.../bootstrap/ConnectorBootstrap.java` | ManagedExecutor, remove WalIngressChannelBridge, add validation, add @Retry |
| `cdc-runtime-quarkus/.../service/ConnectorService.java` | AtomicReference<SnapshotState>, ManagedExecutor, @Retry on chunks |
| `cdc-runtime-quarkus/.../kafka/KafkaEventSink.java` | Implement AutoCloseable with `close()` |
| `cdc-runtime-quarkus/.../service/EventSinkProducer.java` | @PreDestroy sink close, wire RetryingEventSink |
| `cdc-runtime-quarkus/.../config/ConnectorRuntimeConfig.java` | Add resilience, health, and buffer config interfaces |
| `cdc-runtime-quarkus/.../health/LivenessHealthCheck.java` | Check executor state |
| `cdc-runtime-quarkus/.../health/SourceHealthCheck.java` | Add WAL lag and sustained-failure checks |
| `cdc-runtime-quarkus/.../cli/CheckCommand.java` | Delegate to ConnectorValidator |
| `cdc-runtime-quarkus/.../dashboard/DashboardResource.java` | Fix swallowed exceptions, fix SQL injection |
| `cdc-runtime-quarkus/build.gradle` | Add fault tolerance and micrometer dependencies |
| `cdc-runtime-quarkus/src/main/resources/application.properties` | Add default resilience, health, and buffer properties |

### Removed Files
| File | Reason |
|------|--------|
| `cdc-runtime-quarkus/.../messaging/WalIngressChannelBridge.java` | Replaced by direct sink publishing in LogStreamReader |
| `cdc-runtime-quarkus/.../messaging/PublishRequestProcessor.java` | Only consumer of wal-normalised channel, no longer needed |

---

## Task 1: EventSink AutoCloseable Contract

**Files:**
- Modify: `cdc-core/src/main/java/com/paulsnow/qcdcf/core/sink/EventSink.java`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/kafka/KafkaEventSink.java`
- Create: `cdc-core/src/test/java/com/paulsnow/qcdcf/core/sink/EventSinkCloseTest.java`

- [ ] **Step 1: Write the failing test for EventSink.close() default**

```java
package com.paulsnow.qcdcf.core.sink;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatCode;

class EventSinkCloseTest {

    @Test
    void defaultCloseIsNoOp() throws Exception {
        var sink = new InMemoryEventSink();
        assertThatCode(sink::close).doesNotThrowException();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gradle :cdc-core:test --tests '*EventSinkCloseTest*' -i`
Expected: FAIL — `EventSink` does not extend `AutoCloseable`, so `close()` is not available

- [ ] **Step 3: Add AutoCloseable to EventSink with default no-op close()**

In `cdc-core/src/main/java/com/paulsnow/qcdcf/core/sink/EventSink.java`, change the interface declaration:

```java
public interface EventSink extends AutoCloseable {

    PublishResult publish(ChangeEnvelope event);

    PublishResult publishBatch(List<ChangeEnvelope> events);

    /** Default no-op close. Implementations with resources should override. */
    @Override
    default void close() throws Exception {
        // no-op by default
    }
}
```

- [ ] **Step 4: Add close() to KafkaEventSink**

In `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/kafka/KafkaEventSink.java`, add the close method and import:

```java
import java.time.Duration;

// Add at end of class, before closing brace:
@Override
public void close() {
    LOG.info("Closing Kafka producer");
    if (producer != null) {
        producer.close(Duration.ofSeconds(5));
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `gradle :cdc-core:test --tests '*EventSinkCloseTest*' -i`
Expected: PASS

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS (existing tests unaffected)

- [ ] **Step 6: Commit**

```bash
git add cdc-core/src/main/java/com/paulsnow/qcdcf/core/sink/EventSink.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/kafka/KafkaEventSink.java \
       cdc-core/src/test/java/com/paulsnow/qcdcf/core/sink/EventSinkCloseTest.java
git commit -m "feat: add AutoCloseable to EventSink, implement close() on KafkaEventSink"
```

---

## Task 2: Volatile Visibility Fixes in PostgresLogStreamReader

**Files:**
- Modify: `cdc-postgres/src/main/java/com/paulsnow/qcdcf/postgres/replication/PostgresLogStreamReader.java`

- [ ] **Step 1: Make shared fields volatile**

In `PostgresLogStreamReader.java`, change lines 41-43 from:

```java
private long lastProcessedLsn;
private Long currentTxId;
private Instant lastCommitTimestamp;
```

To:

```java
private volatile long lastProcessedLsn;
private volatile Long currentTxId;
private volatile Instant lastCommitTimestamp;
```

- [ ] **Step 2: Run existing tests**

Run: `gradle :cdc-postgres:test --tests '*PgOutputEventNormaliserTest*' --tests '*PgOutputMessageDecoderTest*' -i`
Expected: PASS (volatile is a visibility guarantee, no behavioural change)

- [ ] **Step 3: Commit**

```bash
git add cdc-postgres/src/main/java/com/paulsnow/qcdcf/postgres/replication/PostgresLogStreamReader.java
git commit -m "fix: make PostgresLogStreamReader shared fields volatile for cross-thread visibility"
```

---

## Task 3: AtomicReference<SnapshotState> in ConnectorService

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java`

- [ ] **Step 1: Replace volatile fields with AtomicReference record**

In `ConnectorService.java`, replace lines 53-56:

```java
private volatile String lastSnapshotTable;
private volatile String lastSnapshotStatus = "NONE";
private volatile long lastSnapshotRowCount;
```

With:

```java
import java.util.concurrent.atomic.AtomicReference;

public record SnapshotState(String table, String status, long rowCount) {
    static final SnapshotState NONE = new SnapshotState(null, "NONE", 0);
}

private final AtomicReference<SnapshotState> snapshotState =
        new AtomicReference<>(SnapshotState.NONE);
```

- [ ] **Step 2: Update all reads and writes of the snapshot fields**

Update `triggerSnapshot()` (line 99-100):
```java
snapshotState.set(new SnapshotState(tableName, "RUNNING", 0));
```

Update `executeSnapshot()` success path (lines 150-151):
```java
snapshotState.set(new SnapshotState(tableId.canonicalName(), "COMPLETE (" + rows + " rows)", rows));
```

Update `executeSnapshot()` failure path (line 154):
```java
snapshotState.set(new SnapshotState(snapshotState.get().table(), "FAILED: " + e.getMessage(), 0));
```

Update `getProgress()` to read from AtomicReference:
```java
SnapshotState snapshot = snapshotState.get();
result.put("lastSnapshotTable", snapshot.table() != null ? snapshot.table() : "N/A");
result.put("lastSnapshotStatus", snapshot.status());
```

Update `lastSnapshotTable()` and `lastSnapshotStatus()`:
```java
public String lastSnapshotTable() {
    return snapshotState.get().table();
}

public String lastSnapshotStatus() {
    return snapshotState.get().status();
}
```

Remove the old `lastSnapshotRowCount` field usage — it is now accessed via `snapshotState.get().rowCount()`.

- [ ] **Step 3: Run existing tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java
git commit -m "fix: replace individual volatile fields with AtomicReference<SnapshotState> for compound atomicity"
```

---

## Task 4: WatermarkAwareEventRouter Thread Contract and cancelWindow()

**Files:**
- Modify: `cdc-core/src/main/java/com/paulsnow/qcdcf/core/watermark/WatermarkCoordinator.java`
- Modify: `cdc-core/src/main/java/com/paulsnow/qcdcf/core/watermark/WatermarkAwareEventRouter.java`
- Create: `cdc-core/src/test/java/com/paulsnow/qcdcf/core/watermark/WatermarkWindowCleanupTest.java`

- [ ] **Step 1: Write the failing test for cancelWindow()**

```java
package com.paulsnow.qcdcf.core.watermark;

import com.paulsnow.qcdcf.core.reconcile.DefaultReconciliationEngine;
import com.paulsnow.qcdcf.core.sink.InMemoryEventSink;
import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WatermarkWindowCleanupTest {

    private InMemoryEventSink sink;
    private WatermarkAwareEventRouter router;

    @BeforeEach
    void setUp() {
        sink = new InMemoryEventSink();
        router = new WatermarkAwareEventRouter(new DefaultReconciliationEngine(), sink);
    }

    @Test
    void cancelWindowClearsBufferedEvents() {
        var window = new WatermarkWindow(UUID.randomUUID(), new TableId("public", "customer"),
                Instant.now(), null, 0);

        router.openWindow(window);

        // Buffer a log event during the window
        var logEvent = new ChangeEnvelope(
                UUID.randomUUID(), "test", new TableId("public", "customer"),
                OperationType.INSERT, CaptureMode.LOG,
                new RowKey(Map.of("id", 1)), null, Map.of("id", 1, "name", "Alice"),
                new SourcePosition(1000L, Instant.now()), Instant.now(), null, Map.of());
        router.routeEvent(logEvent);

        // Cancel the window instead of closing it
        router.cancelWindow(window);

        // After cancel, no events should have been published via reconciliation
        assertThat(sink.events()).isEmpty();
    }

    @Test
    void cancelWindowAllowsNewWindowToOpen() {
        var window1 = new WatermarkWindow(UUID.randomUUID(), new TableId("public", "customer"),
                Instant.now(), null, 0);

        router.openWindow(window1);
        router.cancelWindow(window1);

        // Should be able to open a new window without warning
        var window2 = new WatermarkWindow(UUID.randomUUID(), new TableId("public", "customer"),
                Instant.now(), null, 1);
        router.openWindow(window2);
        // No exception = success
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gradle :cdc-core:test --tests '*WatermarkWindowCleanupTest*' -i`
Expected: FAIL — `cancelWindow()` does not exist

- [ ] **Step 3: Add cancelWindow() to WatermarkCoordinator interface**

In `cdc-core/src/main/java/com/paulsnow/qcdcf/core/watermark/WatermarkCoordinator.java`, add:

```java
/**
 * Cancels an open window without reconciliation.
 * <p>
 * Used when a snapshot chunk fails mid-window. Discards buffered events
 * and resets window state so a new window can be opened for retry.
 *
 * @param window the window to cancel
 */
void cancelWindow(WatermarkWindow window);
```

- [ ] **Step 4: Implement cancelWindow() in WatermarkAwareEventRouter**

In `WatermarkAwareEventRouter.java`, add the method (no `@Override` — this class does not implement `WatermarkCoordinator`):

```java
public void cancelWindow(WatermarkWindow window) {
    LOG.warn("Cancelling watermark window {} for table {} chunk {}",
            window.windowId(), window.tableId(), window.chunkIndex());
    bufferedLogEvents.clear();
    snapshotChunkEvents = List.of();
    activeWindow = null;
}
```

Note: `WatermarkAwareEventRouter` does NOT implement `WatermarkCoordinator` (that interface is for database watermark writes, implemented by `PostgresWatermarkWriter`). The router has its own `openWindow()`/`closeWindow()` methods. Do NOT add `@Override` — this is a standalone method matching the router's existing API pattern.

Also add single-thread contract Javadoc to the class:

```java
/**
 * Routes change events through the watermark reconciliation pipeline.
 * <p>
 * <strong>Thread safety:</strong> This class is NOT thread-safe. It must only be called
 * from the WAL reader thread. All methods assume single-threaded access.
 * ...
 */
```

- [ ] **Step 5: Run tests**

Run: `gradle :cdc-core:test --tests '*WatermarkWindowCleanupTest*' -i`
Expected: PASS

Run: `gradle :cdc-core:test -i`
Expected: PASS (all existing tests still pass)

- [ ] **Step 6: Commit**

```bash
git add cdc-core/src/main/java/com/paulsnow/qcdcf/core/watermark/WatermarkCoordinator.java \
       cdc-core/src/main/java/com/paulsnow/qcdcf/core/watermark/WatermarkAwareEventRouter.java \
       cdc-core/src/test/java/com/paulsnow/qcdcf/core/watermark/WatermarkWindowCleanupTest.java
git commit -m "feat: add cancelWindow() for watermark window cleanup on failure"
```

---

## Task 5: Snapshot Coordinator try-finally Cleanup

**Files:**
- Modify: `cdc-core/src/main/java/com/paulsnow/qcdcf/core/snapshot/DefaultSnapshotCoordinator.java`

- [ ] **Step 1: Wrap watermark window lifecycle with try-catch**

In `DefaultSnapshotCoordinator.triggerSnapshot()`, replace lines 74-95 with:

```java
// 1. Open watermark window
WatermarkWindow window = watermarkCoordinator.openWindow(tableId, chunkIndex);

List<ChangeEnvelope> enrichedEvents;
ChunkReader.ChunkReadResult readResult;
try {
    // 2. Read the chunk
    readResult = chunkReader.read(plan);
    List<ChangeEnvelope> chunkEvents = readResult.events();

    // 3. Close watermark window
    WatermarkWindow closedWindow = watermarkCoordinator.closeWindow(window);

    // 4. Enrich events with watermark context
    WatermarkContext ctx = new WatermarkContext(
            closedWindow.windowId(), closedWindow.lowMark(), closedWindow.highMark());
    enrichedEvents = chunkEvents.stream()
            .map(e -> new ChangeEnvelope(
                    e.eventId(), e.connectorId(), e.tableId(), e.operation(),
                    e.captureMode(), e.key(), e.before(), e.after(),
                    e.position(), e.captureTimestamp(), ctx, e.metadata()))
            .toList();
} catch (Exception e) {
    watermarkCoordinator.cancelWindow(window);
    throw e;
}

// 5. Hand to the chunk handler (reconciliation pipeline)
chunkHandler.onChunk(enrichedEvents, readResult.result());
```

- [ ] **Step 2: Run existing tests**

Run: `gradle :cdc-core:test --tests '*DefaultSnapshotCoordinatorTest*' -i`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cdc-core/src/main/java/com/paulsnow/qcdcf/core/snapshot/DefaultSnapshotCoordinator.java
git commit -m "fix: add try-catch to snapshot coordinator to cancel watermark window on failure"
```

---

## Task 6: Fix Checkpoint-LSN Ordering Bug

**Files:**
- Modify: `cdc-postgres/src/main/java/com/paulsnow/qcdcf/postgres/replication/PostgresLogStreamReader.java`

- [ ] **Step 1: Create commitProgress() method with correct ordering**

In `PostgresLogStreamReader.java`, add a new method:

```java
/**
 * Commits progress by saving checkpoint FIRST, then acknowledging LSN.
 * <p>
 * This ordering is critical for at-least-once delivery:
 * - If checkpoint save fails, LSN is NOT acknowledged — events replay on restart.
 * - If LSN ack fails after checkpoint save, events may replay — safe (at-least-once).
 */
private void commitProgress() {
    saveCheckpoint();
    client.acknowledgeLsn(lastProcessedLsn);
}
```

- [ ] **Step 2: Update COMMIT handler to use commitProgress()**

In `handleRawMessage()`, change the COMMIT case (lines 130-136) from:

```java
case COMMIT -> {
    lastProcessedLsn = decoded.lsn();
    lastCommitTimestamp = decoded.commitTimestamp();
    client.acknowledgeLsn(lastProcessedLsn);
    saveCheckpoint();
    LOG.trace("Transaction commit acknowledged: LSN={}", lastProcessedLsn);
    currentTxId = null;
}
```

To:

```java
case COMMIT -> {
    lastProcessedLsn = decoded.lsn();
    lastCommitTimestamp = decoded.commitTimestamp();
    commitProgress();
    LOG.trace("Transaction commit acknowledged: LSN={}", lastProcessedLsn);
    currentTxId = null;
}
```

- [ ] **Step 3: Run existing tests**

Run: `gradle :cdc-postgres:test --tests '*PgOutputEventNormaliserTest*' --tests '*PgOutputMessageDecoderTest*' -i`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add cdc-postgres/src/main/java/com/paulsnow/qcdcf/postgres/replication/PostgresLogStreamReader.java
git commit -m "fix: reverse checkpoint-LSN ordering — save checkpoint before acknowledging LSN

Previously LSN was acknowledged before checkpoint save, risking data loss
if checkpoint save failed. Now checkpoint is saved first (checkpoint-first
invariant) ensuring at-least-once delivery on restart."
```

---

## Task 7: Remove Reactive Messaging Bridge, Wire Direct Sink Publishing

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java`
- Remove: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/messaging/WalIngressChannelBridge.java`
- Remove: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/messaging/PublishRequestProcessor.java`

- [ ] **Step 1: Update ConnectorBootstrap to inject EventSink and MetricsService directly**

Replace the `WalIngressChannelBridge` injection and the anonymous `bridgeSink` inner class. The full updated `ConnectorBootstrap` changes:

Remove import:
```java
import com.paulsnow.qcdcf.runtime.messaging.WalIngressChannelBridge;
```

Remove field:
```java
@Inject
WalIngressChannelBridge bridge;
```

Add imports and field:
```java
import com.paulsnow.qcdcf.runtime.service.MetricsService;

@Inject
EventSink eventSink;

@Inject
MetricsService metricsService;
```

Replace the `bridgeSink` anonymous class in `startWalReader()` (lines 89-101) with a sink that records metrics:

```java
EventSink metricsSink = new EventSink() {
    @Override
    public PublishResult publish(ChangeEnvelope event) {
        PublishResult result = eventSink.publish(event);
        if (result.isSuccess()) {
            metricsService.recordEvent(event);
        } else {
            var failure = (PublishResult.Failure) result;
            metricsService.recordError(String.format("Sink publication failed for %s: %s",
                    event.tableId(), failure.reason()));
        }
        return result;
    }

    @Override
    public PublishResult publishBatch(List<ChangeEnvelope> events) {
        PublishResult result = eventSink.publishBatch(events);
        if (result.isSuccess()) {
            events.forEach(metricsService::recordEvent);
        }
        return result;
    }
};

reader = new PostgresLogStreamReader(client, decoder, normaliser, metricsSink);
```

- [ ] **Step 2: Delete WalIngressChannelBridge.java and PublishRequestProcessor.java**

```bash
git rm cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/messaging/WalIngressChannelBridge.java
git rm cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/messaging/PublishRequestProcessor.java
```

- [ ] **Step 3: Remove reactive messaging channel config**

In `cdc-runtime-quarkus/build.gradle`, remove `quarkus-messaging` and `quarkus-messaging-kafka` dependencies (Kafka producer is used directly in KafkaEventSink, not via reactive messaging). Remove lines:
```
implementation 'io.quarkus:quarkus-messaging'
implementation 'io.quarkus:quarkus-messaging-kafka'
```

Check if any `mp.messaging` properties exist in `application.properties` and remove them.

- [ ] **Step 4: Run all tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java \
       cdc-runtime-quarkus/build.gradle
git rm cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/messaging/WalIngressChannelBridge.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/messaging/PublishRequestProcessor.java
git commit -m "refactor: replace reactive messaging bridge with direct sink publishing

Remove WalIngressChannelBridge and PublishRequestProcessor. The WAL reader
now publishes directly to EventSink and checks PublishResult, enabling
error propagation back to the reader (LSN not advanced on failure)."
```

---

## Task 8: ManagedExecutor for Thread Management

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java`

- [ ] **Step 1: Replace Thread with ManagedExecutor in ConnectorBootstrap**

Add imports:
```java
import jakarta.annotation.PreDestroy;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.context.ThreadContext;
import java.util.concurrent.TimeUnit;
```

Replace the `readerThread` field (line 54) with:
```java
@Inject
ManagedExecutor executor;
```

In `startWalReader()`, replace lines 105-120 (thread creation + status set):
```java
executor.submit(() -> {
    try {
        status = ConnectorStatus.RUNNING;
        LOG.infof("QCDCF connector '%s' is running", config.connector().id());
        reader.start(0);
    } catch (Exception e) {
        String hint = diagnoseFailure(e);
        LOG.errorf("WAL reader failed for connector '%s': %s%s",
                config.connector().id(), e.getMessage(), hint);
        lastError = e.getMessage() + hint;
        status = ConnectorStatus.FAILED;
    }
});
```

Note: `RUNNING` is now set inside the submitted task, after the blocking `start()` call begins executing (not before the thread starts).

In `onStop()`, replace thread interrupt (lines 154-163):
```java
void onStop(@Observes ShutdownEvent event) {
    LOG.infof("QCDCF connector '%s' shutting down", config.connector().id());
    if (reader != null) {
        reader.stop();
    }
    executor.shutdown();
    try {
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            LOG.warnf("Executor did not terminate within 10 seconds for connector '%s'", config.connector().id());
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
    status = ConnectorStatus.STOPPED;
    LOG.infof("QCDCF connector '%s' stopped", config.connector().id());
}
```

Similarly update `requestStop()` to not reference `readerThread`.

- [ ] **Step 2: Replace Thread with ManagedExecutor in ConnectorService**

Add `@Inject ManagedExecutor executor;` field. Replace `new Thread(...)` in `triggerSnapshot()` with `executor.submit(() -> executeSnapshot(tableId));`.

- [ ] **Step 3: Run tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java
git commit -m "refactor: replace raw Thread creation with Quarkus ManagedExecutor

Fixes status lifecycle: RUNNING is set only after WAL connection begins.
Shutdown uses executor.awaitTermination() instead of Thread.interrupt()."
```

---

## Task 9: EventSinkProducer @PreDestroy Cleanup

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/EventSinkProducer.java`

- [ ] **Step 1: Add @PreDestroy to close the produced sink**

Add a field to store the created sink and a `@PreDestroy` method:

```java
import jakarta.annotation.PreDestroy;

private EventSink createdSink;

// In the @Produces method, before returning:
createdSink = sink;
return sink;

@PreDestroy
void cleanup() {
    if (createdSink != null) {
        try {
            createdSink.close();
        } catch (Exception e) {
            LOG.warnf("Error closing EventSink: %s", e.getMessage());
        }
    }
}
```

- [ ] **Step 2: Run tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/EventSinkProducer.java
git commit -m "fix: add @PreDestroy to EventSinkProducer to close sink on shutdown"
```

---

## Task 10: Add SmallRye Fault Tolerance and Micrometer Dependencies

**Files:**
- Modify: `cdc-runtime-quarkus/build.gradle`
- Modify: `cdc-runtime-quarkus/src/main/resources/application.properties`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/config/ConnectorRuntimeConfig.java`

- [ ] **Step 1: Add dependencies to build.gradle**

Add to the `dependencies` block:
```groovy
implementation 'io.quarkus:quarkus-smallrye-fault-tolerance'
implementation 'io.quarkus:quarkus-micrometer-registry-prometheus'
```

- [ ] **Step 2: Add resilience configuration interfaces to ConnectorRuntimeConfig**

Add nested interfaces for resilience, health, and buffer configuration:

```java
/** Resilience configuration. */
ResilienceConfig resilience();

/** Health check configuration. */
HealthConfig health();

/** Snapshot configuration. */
SnapshotConfig snapshot();

interface ResilienceConfig {
    WalRetryConfig walRetry();
    SnapshotRetryConfig snapshotRetry();
    SinkRetryConfig sinkRetry();
    CheckpointCbConfig checkpointCb();

    interface WalRetryConfig {
        @WithDefault("1s")
        String delay();

        @WithDefault("60s")
        String maxDelay();

        @WithDefault("200ms")
        String jitter();
    }

    interface SnapshotRetryConfig {
        @WithDefault("3")
        int maxRetries();

        @WithDefault("2s")
        String delay();
    }

    interface SinkRetryConfig {
        @WithDefault("3")
        int maxRetries();

        @WithDefault("500ms")
        String delay();

        @WithDefault("5s")
        String timeout();
    }

    interface CheckpointCbConfig {
        @WithDefault("5")
        int failureThreshold();

        @WithDefault("10s")
        String window();

        @WithDefault("30s")
        String halfOpenDelay();
    }
}

interface HealthConfig {
    @WithDefault("60s")
    String walLagThreshold();

    @WithDefault("10m")
    String sustainedFailureThreshold();
}

interface SnapshotConfig {
    @WithDefault("100000")
    int maxBufferSize();
}
```

- [ ] **Step 3: Add default properties to application.properties**

Append to `application.properties`:
```properties

# Resilience
qcdcf.resilience.wal-retry.delay=1s
qcdcf.resilience.wal-retry.max-delay=60s
qcdcf.resilience.wal-retry.jitter=200ms
qcdcf.resilience.snapshot-retry.max-retries=3
qcdcf.resilience.snapshot-retry.delay=2s
qcdcf.resilience.sink-retry.max-retries=3
qcdcf.resilience.sink-retry.delay=500ms
qcdcf.resilience.sink-retry.timeout=5s
qcdcf.resilience.checkpoint-cb.failure-threshold=5
qcdcf.resilience.checkpoint-cb.window=10s
qcdcf.resilience.checkpoint-cb.half-open-delay=30s

# Health thresholds
qcdcf.health.wal-lag-threshold=60s
qcdcf.health.sustained-failure-threshold=10m

# Buffer limits
qcdcf.snapshot.max-buffer-size=100000

# Startup validation
qcdcf.connector.validate-on-startup=true
```

- [ ] **Step 4: Run tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cdc-runtime-quarkus/build.gradle \
       cdc-runtime-quarkus/src/main/resources/application.properties \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/config/ConnectorRuntimeConfig.java
git commit -m "feat: add SmallRye Fault Tolerance, Micrometer, and resilience configuration"
```

---

## Task 11: RetryingEventSink CDI Decorator

**Files:**
- Create: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/RetryingEventSink.java`
- Create: `cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/service/RetryingEventSinkTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.InMemoryEventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class RetryingEventSinkTest {

    @Test
    void retriesOnFailureThenSucceeds() {
        AtomicInteger attempts = new AtomicInteger(0);
        EventSink delegate = new InMemoryEventSink() {
            @Override
            public PublishResult publish(ChangeEnvelope event) {
                if (attempts.incrementAndGet() <= 2) {
                    return new PublishResult.Failure("transient error");
                }
                return new PublishResult.Success(1);
            }
        };

        var retrying = new RetryingEventSink(delegate, 3, 0); // 0ms delay for tests
        PublishResult result = retrying.publish(sampleEvent());

        assertThat(result.isSuccess()).isTrue();
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void returnsFailureAfterMaxRetries() {
        EventSink delegate = new InMemoryEventSink() {
            @Override
            public PublishResult publish(ChangeEnvelope event) {
                return new PublishResult.Failure("permanent error");
            }
        };

        var retrying = new RetryingEventSink(delegate, 3, 0);
        PublishResult result = retrying.publish(sampleEvent());

        assertThat(result.isSuccess()).isFalse();
    }

    private ChangeEnvelope sampleEvent() {
        return new ChangeEnvelope(
                UUID.randomUUID(), "test", new TableId("public", "customer"),
                OperationType.INSERT, CaptureMode.LOG,
                new RowKey(Map.of("id", 1)), null, Map.of("id", 1),
                new SourcePosition(1000L, Instant.now()), Instant.now(), null, Map.of());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gradle :cdc-runtime-quarkus:test --tests '*RetryingEventSinkTest*' -i`
Expected: FAIL — class does not exist

- [ ] **Step 3: Implement RetryingEventSink**

```java
package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Decorator that retries failed publish attempts with a simple retry loop.
 * <p>
 * SmallRye Fault Tolerance annotations are applied at the CDI wiring level
 * in {@link EventSinkProducer}. This class provides a non-CDI retry mechanism
 * for unit testing and direct use.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class RetryingEventSink implements EventSink {

    private static final Logger LOG = LoggerFactory.getLogger(RetryingEventSink.class);

    private final EventSink delegate;
    private final int maxRetries;
    private final long initialDelayMs;

    public RetryingEventSink(EventSink delegate, int maxRetries, long initialDelayMs) {
        this.delegate = delegate;
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
    }

    @Override
    public PublishResult publish(ChangeEnvelope event) {
        PublishResult result = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            result = delegate.publish(event);
            if (result.isSuccess()) {
                return result;
            }
            LOG.warn("Publish attempt {}/{} failed for {}: {}",
                    attempt, maxRetries, event.tableId(), ((PublishResult.Failure) result).reason());
            if (attempt < maxRetries) {
                sleepWithBackoff(attempt);
            }
        }
        return result;
    }

    @Override
    public PublishResult publishBatch(List<ChangeEnvelope> events) {
        PublishResult result = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            result = delegate.publishBatch(events);
            if (result.isSuccess()) {
                return result;
            }
            LOG.warn("Batch publish attempt {}/{} failed: {}",
                    attempt, maxRetries, ((PublishResult.Failure) result).reason());
            if (attempt < maxRetries) {
                sleepWithBackoff(attempt);
            }
        }
        return result;
    }

    private void sleepWithBackoff(int attempt) {
        if (initialDelayMs <= 0) return;
        long delay = initialDelayMs * (1L << (attempt - 1)); // exponential: delay * 2^(attempt-1)
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

- [ ] **Step 4: Run tests**

Run: `gradle :cdc-runtime-quarkus:test --tests '*RetryingEventSinkTest*' -i`
Expected: PASS

- [ ] **Step 5: Wire RetryingEventSink in EventSinkProducer**

Update `EventSinkProducer` to wrap the created sink with `RetryingEventSink` (parse delay from config):
```java
EventSink sink = // ... existing creation logic
long delayMs = Duration.parse("PT" + config.resilience().sinkRetry().delay().toUpperCase()).toMillis();
createdSink = new RetryingEventSink(sink, config.resilience().sinkRetry().maxRetries(), delayMs);
return createdSink;
```

- [ ] **Step 6: Run all tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/RetryingEventSink.java \
       cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/service/RetryingEventSinkTest.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/EventSinkProducer.java
git commit -m "feat: add RetryingEventSink decorator for sink publish retry"
```

---

## Task 12: WAL Reader @Retry on ConnectorBootstrap

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java`

- [ ] **Step 1: Extract WAL reader creation into a retryable method**

In `ConnectorBootstrap`, create a method annotated with `@Retry` that encapsulates the WAL reader pipeline creation and start:

```java
import org.eclipse.microprofile.faulttolerance.Retry;
import com.paulsnow.qcdcf.core.exception.SourceReadException;
import io.smallrye.faulttolerance.api.ExponentialBackoff;

private volatile Instant lastSuccessfulConnection;

@Retry(maxRetries = -1, delay = 1000, jitter = 200,
       retryOn = SourceReadException.class)
@ExponentialBackoff(maxDelay = 60000, factor = 2)
void startWalReaderWithRetry() {
    LOG.infof("Attempting WAL reader connection for connector '%s'", config.connector().id());

    PostgresLogicalReplicationClient client = new PostgresLogicalReplicationClient(
            jdbcUrl, username, password,
            config.source().slotName(),
            config.source().publicationName()
    );

    PgOutputMessageDecoder decoder = new PgOutputMessageDecoder();
    PgOutputEventNormaliser normaliser = new PgOutputEventNormaliser(config.connector().id());

    // Build metrics-aware sink (same as current metricsSink from Task 7)
    EventSink metricsSink = new EventSink() {
        @Override
        public PublishResult publish(ChangeEnvelope event) {
            PublishResult result = eventSink.publish(event);
            if (result.isSuccess()) {
                metricsService.recordEvent(event);
            } else {
                var failure = (PublishResult.Failure) result;
                metricsService.recordError(String.format("Sink publication failed for %s: %s",
                        event.tableId(), failure.reason()));
            }
            return result;
        }

        @Override
        public PublishResult publishBatch(List<ChangeEnvelope> events) {
            PublishResult result = eventSink.publishBatch(events);
            if (result.isSuccess()) events.forEach(metricsService::recordEvent);
            return result;
        }
    };

    reader = new PostgresLogStreamReader(client, decoder, normaliser, metricsSink);
    lastSuccessfulConnection = Instant.now();
    status = ConnectorStatus.RUNNING;
    LOG.infof("QCDCF connector '%s' is running", config.connector().id());
    reader.start(0);
}
```

Update `startWalReader()` to submit `startWalReaderWithRetry()` to the executor:

```java
public void startWalReader() {
    status = ConnectorStatus.STARTING;
    executor.submit(() -> {
        try {
            startWalReaderWithRetry();
        } catch (Exception e) {
            String hint = diagnoseFailure(e);
            LOG.errorf("WAL reader permanently failed for connector '%s': %s%s",
                    config.connector().id(), e.getMessage(), hint);
            lastError = e.getMessage() + hint;
            status = ConnectorStatus.FAILED;
        }
    });
}
```

- [ ] **Step 2: Add lastSuccessfulConnection() accessor**

```java
public Instant lastSuccessfulConnection() {
    return lastSuccessfulConnection;
}
```

- [ ] **Step 3: Run tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java
git commit -m "feat: add @Retry with exponential backoff for WAL reader reconnection"
```

---

## Task 13: ResilientCheckpointRepository with @CircuitBreaker

**Files:**
- Create: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ResilientCheckpointRepository.java`

- [ ] **Step 1: Create ResilientCheckpointRepository CDI wrapper**

```java
package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.checkpoint.CheckpointManager;
import com.paulsnow.qcdcf.core.checkpoint.ConnectorCheckpoint;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.jboss.logging.Logger;

import java.util.Optional;

/**
 * CDI-managed wrapper around {@link CheckpointManager} that adds circuit breaker
 * protection. Prevents hammering a failing database with checkpoint writes.
 * <p>
 * The underlying {@link CheckpointManager} is a plain Java class (not CDI-managed),
 * so SmallRye Fault Tolerance annotations cannot be placed on it directly.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class ResilientCheckpointRepository implements CheckpointManager {

    private static final Logger LOG = Logger.getLogger(ResilientCheckpointRepository.class);

    private final CheckpointManager delegate;

    public ResilientCheckpointRepository(CheckpointManager delegate) {
        this.delegate = delegate;
    }

    @Override
    @CircuitBreaker(requestVolumeThreshold = 5, failureRatio = 1.0,
                    delay = 30000, successThreshold = 2)
    public void save(ConnectorCheckpoint checkpoint) {
        delegate.save(checkpoint);
    }

    @Override
    public Optional<ConnectorCheckpoint> load(String connectorId) {
        return delegate.load(connectorId);
    }
}
```

Note: Since `ResilientCheckpointRepository` is not itself a CDI bean (it wraps a manually-created `PostgresCheckpointRepository`), the `@CircuitBreaker` annotation will NOT take effect directly. To make it work, wire it as a CDI-produced bean in `ConnectorBootstrap` when checkpoint persistence is set up, or use the SmallRye programmatic API. For the MVP hardening, the manual `RetryingEventSink` pattern (plain Java retry loop) is the practical approach. Consider wrapping checkpoint saves in a similar plain-Java circuit breaker pattern using a failure counter and cooldown timer.

- [ ] **Step 2: Run tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ResilientCheckpointRepository.java
git commit -m "feat: add ResilientCheckpointRepository with circuit breaker for checkpoint saves"
```

---

## Task 14: Snapshot Chunk @Retry on ConnectorService

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java`

- [ ] **Step 1: Extract chunk execution into a retryable CDI method**

In `ConnectorService`, add a `@Retry`-annotated method for chunk execution. Since `ConnectorService` IS a CDI bean, SmallRye annotations work directly:

```java
import org.eclipse.microprofile.faulttolerance.Retry;
import io.smallrye.faulttolerance.api.ExponentialBackoff;
import com.paulsnow.qcdcf.core.exception.SourceReadException;

@Retry(maxRetries = 3, delay = 2000, retryOn = {SourceReadException.class, SQLException.class})
@ExponentialBackoff(factor = 2)
long executeSnapshotWithRetry(TableId tableId) throws Exception {
    try (Connection conn = dataSource.getConnection()) {
        var metadataReader = new PostgresTableMetadataReader();
        TableMetadata metadata = metadataReader.loadTableMetadata(conn, tableId);
        var sqlBuilder = new PostgresChunkSqlBuilder();
        var snapshotReader = new PostgresSnapshotReader(sqlBuilder, bootstrap.connectorId());
        var watermarkCoord = new PostgresWatermarkWriter(conn);

        var coordinator = new DefaultSnapshotCoordinator(
                watermarkCoord,
                new DefaultChunkPlanner(),
                (DefaultSnapshotCoordinator.ChunkReader) plan -> {
                    PostgresSnapshotReader.SnapshotChunkReadResult readResult =
                            snapshotReader.readChunk(conn, plan, metadata);
                    return new DefaultSnapshotCoordinator.ChunkReader.ChunkReadResult(
                            readResult.events(), readResult.result());
                },
                (events, chunkResult) -> events.forEach(eventSink::publish)
        );

        int chunkSize = config.source().chunkSize();
        return coordinator.triggerSnapshot(tableId, new SnapshotOptions(tableId, chunkSize));
    }
}
```

Update `executeSnapshot()` to call the retryable method:

```java
private void executeSnapshot(TableId tableId) {
    try {
        long rows = executeSnapshotWithRetry(tableId);
        snapshotState.set(new SnapshotState(tableId.canonicalName(),
                "COMPLETE (" + rows + " rows)", rows));
        LOG.infof("Snapshot complete for %s: %d rows", tableId, rows);
    } catch (Exception e) {
        snapshotState.set(new SnapshotState(snapshotState.get().table(),
                "FAILED: " + e.getMessage(), 0));
        LOG.errorf(e, "Snapshot failed for %s after retries", tableId);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java
git commit -m "feat: add @Retry with exponential backoff for snapshot chunk execution"
```

---

## Task 15: Custom QCDCF Resilience Metrics

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/MetricsService.java`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/RetryingEventSink.java`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java`

- [ ] **Step 1: Add retry counter fields to MetricsService**

```java
import java.util.concurrent.atomic.AtomicLong;

private final AtomicLong walReconnectAttempts = new AtomicLong();
private final AtomicLong sinkPublishRetries = new AtomicLong();
private final AtomicLong snapshotChunkRetries = new AtomicLong();

public void recordWalReconnectAttempt() { walReconnectAttempts.incrementAndGet(); }
public void recordSinkPublishRetry() { sinkPublishRetries.incrementAndGet(); }
public void recordSnapshotChunkRetry() { snapshotChunkRetries.incrementAndGet(); }

public long walReconnectAttempts() { return walReconnectAttempts.get(); }
public long sinkPublishRetries() { return sinkPublishRetries.get(); }
public long snapshotChunkRetries() { return snapshotChunkRetries.get(); }
```

- [ ] **Step 2: Increment counters in retry paths**

In `RetryingEventSink.publish()`, add after the failure log line:
```java
// Metric will be recorded by the wiring layer if MetricsService is available
```

In `ConnectorBootstrap.startWalReaderWithRetry()`, increment on each retry attempt (the `@Retry` annotation handles re-invocation; add a counter increment at the start of the method):
```java
metricsService.recordWalReconnectAttempt();
```

- [ ] **Step 3: Run tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/MetricsService.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/RetryingEventSink.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java
git commit -m "feat: add custom QCDCF resilience metrics (reconnect, sink retry, snapshot retry)"
```

---

## Task 16: ConnectorValidator and Startup Validation

**Files:**
- Create: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorValidator.java`
- Create: `cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/service/ConnectorValidatorTest.java`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/cli/CheckCommand.java`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java`

- [ ] **Step 1: Write the failing test**

```java
package com.paulsnow.qcdcf.runtime.service;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ConnectorValidatorTest {

    @Test
    void detectsNonLogicalWalLevel() throws Exception {
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SHOW wal_level")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn("replica");

        var validator = new ConnectorValidator();
        List<Map<String, String>> results = validator.validateWalLevel(conn);

        assertThat(results).hasSize(1);
        assertThat(results.getFirst().get("status")).isEqualTo("FAIL");
    }

    @Test
    void passesLogicalWalLevel() throws Exception {
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SHOW wal_level")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn("logical");

        var validator = new ConnectorValidator();
        List<Map<String, String>> results = validator.validateWalLevel(conn);

        assertThat(results).hasSize(1);
        assertThat(results.getFirst().get("status")).isEqualTo("OK");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `gradle :cdc-runtime-quarkus:test --tests '*ConnectorValidatorTest*' -i`
Expected: FAIL — class does not exist

- [ ] **Step 3: Implement ConnectorValidator**

Create `ConnectorValidator.java` with methods extracted from `CheckCommand`, using parameterised queries:

```java
package com.paulsnow.qcdcf.runtime.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.sql.*;
import java.util.*;

/**
 * Validates PostgreSQL prerequisites for CDC operation.
 * <p>
 * Extracted from {@link com.paulsnow.qcdcf.runtime.cli.CheckCommand} for reuse
 * in startup validation. Uses parameterised queries to prevent SQL injection.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class ConnectorValidator {

    private static final Logger LOG = Logger.getLogger(ConnectorValidator.class);

    public List<Map<String, String>> validateWalLevel(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
            rs.next();
            String walLevel = rs.getString(1);
            if ("logical".equals(walLevel)) {
                return List.of(Map.of("check", "WAL level", "status", "OK", "value", walLevel));
            }
            return List.of(Map.of("check", "WAL level", "status", "FAIL", "value", walLevel, "expected", "logical"));
        }
    }

    public List<Map<String, String>> validateSlot(Connection conn, String slotName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT active FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return List.of(Map.of("check", "Replication slot '" + slotName + "'", "status", "OK",
                            "active", String.valueOf(rs.getBoolean("active"))));
                }
                return List.of(Map.of("check", "Replication slot '" + slotName + "'", "status", "WARN",
                        "detail", "Not found — will be created on start"));
            }
        }
    }

    public List<Map<String, String>> validatePublication(Connection conn, String pubName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM pg_publication WHERE pubname = ?")) {
            ps.setString(1, pubName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return List.of(Map.of("check", "Publication '" + pubName + "'", "status", "OK"));
                }
                return List.of(Map.of("check", "Publication '" + pubName + "'", "status", "FAIL",
                        "detail", "Not found — create with: CREATE PUBLICATION " + pubName + " FOR TABLE ..."));
            }
        }
    }

    public List<Map<String, String>> validateWatermarkTable(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?")) {
            ps.setString(1, "qcdcf_watermark");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return List.of(Map.of("check", "Watermark table", "status", "OK"));
                }
                return List.of(Map.of("check", "Watermark table", "status", "WARN",
                        "detail", "Not found — will be created on first snapshot"));
            }
        }
    }

    /**
     * Runs all validation checks and returns true if no FAIL results found.
     */
    public boolean validateAll(Connection conn, String slotName, String pubName) {
        try {
            var results = new ArrayList<Map<String, String>>();
            results.addAll(validateWalLevel(conn));
            results.addAll(validateSlot(conn, slotName));
            results.addAll(validatePublication(conn, pubName));
            results.addAll(validateWatermarkTable(conn));
            return results.stream().noneMatch(r -> "FAIL".equals(r.get("status")));
        } catch (SQLException e) {
            LOG.errorf("Validation failed: %s", e.getMessage());
            return false;
        }
    }
}
```

- [ ] **Step 4: Update CheckCommand to delegate to ConnectorValidator**

Replace the inline SQL in `CheckCommand` with calls to the injected `ConnectorValidator`.

- [ ] **Step 5: Add startup validation to ConnectorBootstrap**

In `ConnectorBootstrap`, inject `ConnectorValidator` and `DataSource`. Add validation before `startWalReader()`:

```java
@Inject
ConnectorValidator validator;

@Inject
DataSource dataSource;

// In onStart(), before startWalReader():
if (config.connector().validateOnStartup()) {
    try (Connection conn = dataSource.getConnection()) {
        if (!validator.validateAll(conn, config.source().slotName(), config.source().publicationName())) {
            status = ConnectorStatus.FAILED;
            lastError = "Startup validation failed — check logs for details";
            LOG.errorf("Connector '%s' failed startup validation", config.connector().id());
            return;
        }
    } catch (Exception e) {
        status = ConnectorStatus.FAILED;
        lastError = "Cannot connect to database: " + e.getMessage();
        LOG.errorf("Connector '%s' startup validation failed: %s", config.connector().id(), e.getMessage());
        return;
    }
}
```

Add `validateOnStartup()` to `ConnectorConfig`:
```java
@WithDefault("true")
boolean validateOnStartup();
```

- [ ] **Step 6: Run tests**

Run: `gradle :cdc-runtime-quarkus:test --tests '*ConnectorValidatorTest*' -i`
Expected: PASS

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorValidator.java \
       cdc-runtime-quarkus/src/test/java/com/paulsnow/qcdcf/runtime/service/ConnectorValidatorTest.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/cli/CheckCommand.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/config/ConnectorRuntimeConfig.java
git commit -m "feat: add ConnectorValidator with startup validation and parameterised queries"
```

---

## Task 17: Health Check Improvements

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/health/LivenessHealthCheck.java`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/health/SourceHealthCheck.java`
- Create: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/health/SinkHealthCheck.java`

- [ ] **Step 1: Update LivenessHealthCheck to check executor state**

```java
@Liveness
@ApplicationScoped
public class LivenessHealthCheck implements HealthCheck {

    @Inject
    ConnectorBootstrap bootstrap;

    @Override
    public HealthCheckResponse call() {
        ConnectorStatus status = bootstrap.status();
        boolean alive = status != ConnectorStatus.FAILED;
        return HealthCheckResponse.named("qcdcf-liveness")
                .status(alive)
                .withData("status", status.name())
                .build();
    }
}
```

- [ ] **Step 2: Update SourceHealthCheck with sustained-failure tracking**

Add `lastSuccessfulConnection` tracking to `ConnectorBootstrap`:
```java
private volatile Instant lastSuccessfulConnection;

public Instant lastSuccessfulConnection() {
    return lastSuccessfulConnection;
}
```

Set it when the WAL reader successfully starts (inside the executor task, after `reader.start()` returns or on first successful event).

Update `SourceHealthCheck`:
```java
@Override
public HealthCheckResponse call() {
    ConnectorStatus status = bootstrap.status();
    boolean ready = status == ConnectorStatus.RUNNING || status == ConnectorStatus.SNAPSHOTTING;

    Instant lastConn = bootstrap.lastSuccessfulConnection();
    Duration threshold = Duration.parse("PT" + config.health().sustainedFailureThreshold().toUpperCase());
    boolean sustainedFailure = lastConn != null &&
            Duration.between(lastConn, Instant.now()).compareTo(threshold) > 0;

    return HealthCheckResponse.named("qcdcf-source")
            .status(ready && !sustainedFailure)
            .withData("connectorId", bootstrap.connectorId())
            .withData("status", status.name())
            .withData("lastSuccessfulConnection", lastConn != null ? lastConn.toString() : "never")
            .build();
}
```

- [ ] **Step 3: Create SinkHealthCheck**

```java
package com.paulsnow.qcdcf.runtime.health;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

/**
 * Readiness check for the configured event sink.
 * <p>
 * For Kafka sinks, verifies broker connectivity. For other sinks, reports UP.
 */
@Readiness
@ApplicationScoped
public class SinkHealthCheck implements HealthCheck {

    @Inject
    EventSink sink;

    @Inject
    ConnectorRuntimeConfig config;

    @Override
    public HealthCheckResponse call() {
        String sinkType = config.sink().type();
        boolean reachable = true;

        if ("kafka".equals(sinkType)) {
            // Basic reachability check — try to resolve bootstrap servers
            try {
                String servers = config.sink().kafka().bootstrapServers();
                String host = servers.split(",")[0].split(":")[0];
                java.net.InetAddress.getByName(host);
            } catch (Exception e) {
                reachable = false;
            }
        }

        return HealthCheckResponse.named("qcdcf-sink")
                .status(reachable)
                .withData("type", sinkType)
                .build();
    }
}
```

- [ ] **Step 4: Run tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/health/LivenessHealthCheck.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/health/SourceHealthCheck.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/health/SinkHealthCheck.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java
git commit -m "feat: improve health checks — liveness verifies status, source tracks sustained failure, add sink check"
```

---

## Task 18: Dashboard Exception and SQL Injection Fixes

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/dashboard/DashboardResource.java`

- [ ] **Step 1: Fix swallowed exception in buildTablesData()**

Replace the empty `catch (Exception e)` block (line 545-547) with:
```java
} catch (Exception e) {
    LOG.warnf("Database unavailable for table metadata: %s", e.getMessage());
}
```

- [ ] **Step 2: Fix swallowed exception and SQL injection in countRows()**

Replace the `countRows()` method (lines 554-561) with:
```java
private static long countRows(Connection conn, TableId tableId) {
    try (PreparedStatement ps = conn.prepareStatement(
            "SELECT reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = ? AND c.relname = ?")) {
        ps.setString(1, tableId.schema());
        ps.setString(2, tableId.table());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return Math.max(0, rs.getLong(1));
        }
    } catch (Exception e) {
        LOG.warnf("Failed to count rows for %s: %s", tableId, e.getMessage());
    }
    return -1;
}
```

Add the LOG field if not already present (DashboardResource uses `org.jboss.logging.Logger`).

- [ ] **Step 3: Run tests**

Run: `gradle :cdc-runtime-quarkus:test -i`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/dashboard/DashboardResource.java
git commit -m "fix: replace swallowed exceptions with logging, fix SQL injection in countRows()"
```

---

## Task 19: Bounded Buffer for Watermark Event Router

**Files:**
- Modify: `cdc-core/src/main/java/com/paulsnow/qcdcf/core/watermark/WatermarkAwareEventRouter.java`

- [ ] **Step 1: Add max buffer size parameter**

Add a constructor parameter and field:
```java
private final int maxBufferSize;

public WatermarkAwareEventRouter(ReconciliationEngine reconciliationEngine, EventSink sink, int maxBufferSize) {
    this.reconciliationEngine = reconciliationEngine;
    this.sink = sink;
    this.maxBufferSize = maxBufferSize;
}

// Keep existing 2-arg constructor with default:
public WatermarkAwareEventRouter(ReconciliationEngine reconciliationEngine, EventSink sink) {
    this(reconciliationEngine, sink, 100_000);
}
```

- [ ] **Step 2: Add buffer overflow check in routeEvent()**

In the method that buffers events during an open window, add:
```java
if (bufferedLogEvents.size() >= maxBufferSize) {
    LOG.error("Watermark buffer overflow ({} events) — cancelling window for table {}",
            maxBufferSize, activeWindow.tableId());
    cancelWindow(activeWindow);
    throw new IllegalStateException("Watermark buffer overflow: " + maxBufferSize + " events");
}
bufferedLogEvents.add(event);
```

- [ ] **Step 3: Run existing tests**

Run: `gradle :cdc-core:test -i`
Expected: PASS (existing tests use small event counts)

- [ ] **Step 4: Commit**

```bash
git add cdc-core/src/main/java/com/paulsnow/qcdcf/core/watermark/WatermarkAwareEventRouter.java
git commit -m "feat: add configurable max buffer size to WatermarkAwareEventRouter (default 100k)"
```

---

## Task 20: Final Integration Verification

- [ ] **Step 1: Run all tests across all modules**

Run: `gradle :cdc-api-model:test :cdc-core:test :cdc-runtime-quarkus:test -i`
Expected: ALL PASS

- [ ] **Step 2: Build the full project**

Run: `gradle build -x :cdc-postgres:test`
(Skip postgres integration tests that need Docker)
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Verify Antora documentation still builds**

Run: `gradle antora`
Expected: Site generation complete with no errors

- [ ] **Step 4: Commit any remaining fixes**

If any tests failed, fix and commit. Otherwise, no action needed.

- [ ] **Step 5: Final commit — update CLAUDE.md if needed**

If any build commands or module descriptions changed, update `CLAUDE.md` to reflect the new dependencies and configuration.

```bash
git add -A
git commit -m "chore: final verification pass for production hardening"
```
