# Scheduled Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add cron-based scheduled snapshots for all published tables, wire up the existing SnapshotMonitorService, and show schedule status in the dashboard.

**Architecture:** New `SnapshotSchedulerService` CDI bean with `@Scheduled` method. Discovers published tables via `PostgresTableMetadataReader`, triggers each via `ConnectorService`. Skip-if-running concurrency rule. Wire `SnapshotMonitorService` recording calls into `ConnectorService.executeSnapshot()`.

**Tech Stack:** Quarkus Scheduler (already available), Java 25

**Spec:** `docs/superpowers/specs/2026-03-29-scheduled-snapshots-design.md`

---

## File Structure

### New Files
| File | Responsibility |
|------|---------------|
| `cdc-runtime-quarkus/.../service/SnapshotSchedulerService.java` | Cron-scheduled snapshot trigger for all published tables |

### Modified Files
| File | Changes |
|------|---------|
| `cdc-runtime-quarkus/.../config/ConnectorRuntimeConfig.java` | Add `ScheduleConfig` to `SnapshotConfig` |
| `cdc-runtime-quarkus/.../service/ConnectorService.java` | Wire `SnapshotMonitorService` calls, add `isSnapshotRunning()` |
| `cdc-runtime-quarkus/src/main/resources/application.properties` | Add schedule defaults |
| `cdc-runtime-quarkus/src/main/resources/templates/fragments/snapshotActive.html` | Add schedule indicator |

---

## Task 1: Add Schedule Configuration

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/config/ConnectorRuntimeConfig.java`
- Modify: `cdc-runtime-quarkus/src/main/resources/application.properties`

- [ ] **Step 1: Add ScheduleConfig to SnapshotConfig**

Read `ConnectorRuntimeConfig.java`. In the existing `SnapshotConfig` interface, add:

```java
interface SnapshotConfig {
    @WithDefault("100000")
    int maxBufferSize();

    ScheduleConfig schedule();

    interface ScheduleConfig {
        @WithDefault("false")
        boolean enabled();

        @WithDefault("0 0 2 * * ?")
        String cron();
    }
}
```

- [ ] **Step 2: Add properties to application.properties**

Append:
```properties

# Snapshot scheduling
qcdcf.snapshot.schedule.enabled=false
qcdcf.snapshot.schedule.cron=0 0 2 * * ?
```

- [ ] **Step 3: Compile**

Run: `gradle :cdc-runtime-quarkus:compileJava`

- [ ] **Step 4: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/config/ConnectorRuntimeConfig.java \
       cdc-runtime-quarkus/src/main/resources/application.properties
git commit -m "feat: add snapshot schedule configuration (enabled, cron)"
```

---

## Task 2: Wire SnapshotMonitorService into ConnectorService

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java`

- [ ] **Step 1: Read ConnectorService.java fully**

- [ ] **Step 2: Inject SnapshotMonitorService**

Add:
```java
@Inject
SnapshotMonitorService snapshotMonitor;
```

- [ ] **Step 3: Add monitoring calls to executeSnapshot()**

In `executeSnapshot()`, add `recordSnapshotStarted` at the start, `recordSnapshotCompleted` on success, `recordSnapshotFailed` on failure:

```java
private void executeSnapshot(TableId tableId) {
    snapshotMonitor.recordSnapshotStarted(tableId.canonicalName());
    try {
        long rows = executeSnapshotWithRetry(tableId);
        snapshotState.set(new SnapshotState(tableId.canonicalName(), "COMPLETE (" + rows + " rows)", rows));
        snapshotMonitor.recordSnapshotCompleted(tableId.canonicalName(), rows);
        LOG.infof("Snapshot complete for %s: %d rows", tableId, rows);
    } catch (Exception e) {
        snapshotState.set(new SnapshotState(snapshotState.get().table(), "FAILED: " + e.getMessage(), 0));
        snapshotMonitor.recordSnapshotFailed(tableId.canonicalName(), e.getMessage());
        LOG.errorf(e, "Snapshot failed for %s after retries", tableId);
    }
}
```

- [ ] **Step 4: Add isSnapshotRunning() method**

```java
public boolean isSnapshotRunning() {
    return "RUNNING".equals(snapshotState.get().status());
}
```

- [ ] **Step 5: Compile and test**

Run: `gradle :cdc-runtime-quarkus:compileJava`
Run: `gradle :cdc-runtime-quarkus:test`

- [ ] **Step 6: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java
git commit -m "feat: wire SnapshotMonitorService into snapshot execution lifecycle"
```

---

## Task 3: Create SnapshotSchedulerService

**Files:**
- Create: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/SnapshotSchedulerService.java`

- [ ] **Step 1: Create the service**

```java
package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.postgres.metadata.PostgresTableMetadataReader;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;

/**
 * Triggers periodic snapshots for all published tables on a configurable cron schedule.
 * <p>
 * Skips execution if a snapshot is already running or if scheduling is disabled.
 * Discovers tables from the PostgreSQL publication and triggers each sequentially.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class SnapshotSchedulerService {

    private static final Logger LOG = Logger.getLogger(SnapshotSchedulerService.class);

    @Inject
    ConnectorRuntimeConfig config;

    @Inject
    ConnectorService connectorService;

    @Inject
    DataSource dataSource;

    @Scheduled(cron = "{qcdcf.snapshot.schedule.cron}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void scheduledSnapshot() {
        if (!config.snapshot().schedule().enabled()) {
            return;
        }

        if (connectorService.isSnapshotRunning()) {
            LOG.warn("Scheduled snapshot skipped — a snapshot is already running");
            return;
        }

        LOG.info("Scheduled snapshot starting — discovering published tables");

        try (Connection conn = dataSource.getConnection()) {
            var metadataReader = new PostgresTableMetadataReader();
            List<TableId> tables = metadataReader.discoverPublicationTables(
                    conn, config.source().publicationName());

            if (tables.isEmpty()) {
                LOG.warn("Scheduled snapshot — no tables found in publication '%s'",
                        config.source().publicationName());
                return;
            }

            LOG.infof("Scheduled snapshot — %d tables to snapshot", tables.size());

            for (TableId tableId : tables) {
                LOG.infof("Scheduled snapshot — triggering for %s", tableId);
                connectorService.triggerSnapshot(tableId.canonicalName());

                // Wait for this snapshot to complete before starting the next
                while (connectorService.isSnapshotRunning()) {
                    Thread.sleep(1000);
                }
            }

            LOG.info("Scheduled snapshot complete — all tables processed");

        } catch (Exception e) {
            LOG.errorf(e, "Scheduled snapshot failed: %s", e.getMessage());
        }
    }
}
```

- [ ] **Step 2: Compile and test**

Run: `gradle :cdc-runtime-quarkus:compileJava`
Run: `gradle :cdc-runtime-quarkus:test`

- [ ] **Step 3: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/SnapshotSchedulerService.java
git commit -m "feat: add SnapshotSchedulerService with cron-based scheduling for all published tables"
```

---

## Task 4: Dashboard Schedule Indicator

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/resources/templates/fragments/snapshotActive.html`

- [ ] **Step 1: Read the current template**

- [ ] **Step 2: Add schedule indicator section**

Add before the snapshot trigger form (`<!-- Snapshot trigger form -->`):

```html
<!-- Schedule Status -->
<div class="mt-4 pt-4 border-t border-gray-700">
    <div class="flex justify-between items-centre">
        <span class="text-gray-400">Schedule</span>
        {#if scheduleEnabled}
        <span class="font-mono text-sm text-emerald-400">{scheduleCron}</span>
        {#else}
        <span class="font-mono text-sm text-gray-500">Disabled</span>
        {/if}
    </div>
</div>
```

- [ ] **Step 3: Pass schedule data from DashboardResource**

Read `DashboardResource.java` and find the method that builds the snapshot active fragment data (likely `buildSnapshotActiveData` or similar). Add:

```java
.data("scheduleEnabled", config.snapshot().schedule().enabled())
.data("scheduleCron", config.snapshot().schedule().cron())
```

- [ ] **Step 4: Compile and test**

Run: `gradle :cdc-runtime-quarkus:compileJava`
Run: `gradle :cdc-runtime-quarkus:test`

- [ ] **Step 5: Commit**

```bash
git add cdc-runtime-quarkus/src/main/resources/templates/fragments/snapshotActive.html \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/dashboard/DashboardResource.java
git commit -m "feat: add schedule status indicator to snapshots dashboard"
```

---

## Task 5: Final Verification

- [ ] **Step 1: Run all tests**

Run: `gradle :cdc-api-model:test :cdc-core:test :cdc-runtime-quarkus:test`

- [ ] **Step 2: Verify Antora docs build**

Run: `gradle antora`

- [ ] **Step 3: Commit any fixes**
