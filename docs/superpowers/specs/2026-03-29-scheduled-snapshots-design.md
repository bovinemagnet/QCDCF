# Scheduled Snapshots Design

**Date:** 2026-03-29
**Status:** Approved
**Author:** Paul Snow

## Context

Snapshots are currently triggered only via REST API (`POST /api/snapshots/trigger`). There is no way to schedule periodic re-syncs of existing data. For production use, operators need automated snapshots to keep downstream systems aligned with the source database — especially after outages, schema changes, or data corrections.

The `SnapshotMonitorService` was built during the MVP but never wired into the snapshot execution flow, so the Snapshots dashboard page shows no real data.

## Design

### Scheduled Snapshot Service

New `SnapshotSchedulerService` CDI bean using Quarkus `@Scheduled` annotation.

**Behaviour on each tick:**
1. Check `qcdcf.snapshot.schedule.enabled` — return immediately if disabled
2. Check if a snapshot is already running (`ConnectorService.snapshotState`) — if status is "RUNNING", log warning and skip
3. Discover all tables in the publication via `PostgresTableMetadataReader.discoverPublicationTables()`
4. For each table, call `ConnectorService.triggerSnapshot(tableName)` and wait for completion before starting the next table
5. Log summary on completion (tables snapshotted, total rows, duration)

**Concurrency rule:** Skip if any snapshot is already running. One snapshot at a time.

**Schedule:** Configurable cron expression, defaults to `0 0 2 * * ?` (2:00 AM daily). Uses Quarkus cron syntax (same as `@Scheduled(cron = "...")`).

### Configuration

Add to existing `SnapshotConfig` interface in `ConnectorRuntimeConfig`:

```java
interface ScheduleConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("0 0 2 * * ?")
    String cron();
}
```

Properties:
```properties
qcdcf.snapshot.schedule.enabled=false
qcdcf.snapshot.schedule.cron=0 0 2 * * ?
```

### Wire SnapshotMonitorService

The `SnapshotMonitorService` already has full tracking methods (`recordSnapshotStarted`, `recordSnapshotChunkCompleted`, `recordSnapshotCompleted`, `recordSnapshotFailed`, `recordWindowOpened`, `recordWindowClosed`) but they are never called.

Add calls in `ConnectorService.executeSnapshot()`:
- `recordSnapshotStarted(tableName)` at the start
- `recordSnapshotCompleted(tableName, rows)` on success
- `recordSnapshotFailed(tableName, error)` on failure

This makes the existing Snapshots dashboard page functional with real data.

### Dashboard Integration

Add a "Schedule" row to the existing snapshots status fragment showing:
- "Disabled" if `schedule.enabled=false`
- Next fire time and cron expression if enabled

### Files

**New:**
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/SnapshotSchedulerService.java`

**Modified:**
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/config/ConnectorRuntimeConfig.java` — add `ScheduleConfig` to `SnapshotConfig`
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java` — wire `SnapshotMonitorService` calls into `executeSnapshot()`
- `cdc-runtime-quarkus/src/main/resources/application.properties` — add schedule defaults
- `cdc-runtime-quarkus/src/main/resources/templates/fragments/snapshotActive.html` — add schedule indicator

### Dependencies

None — `quarkus-scheduler` is already in `build.gradle` and actively used by `MetricsService` and `ReplicationHealthService`.

## Verification

- `gradle :cdc-runtime-quarkus:test` passes
- With `schedule.enabled=true` and a short cron (e.g., every minute), snapshots fire automatically
- With `schedule.enabled=false` (default), no snapshots fire
- When a snapshot is already running, the scheduler logs a warning and skips
- Snapshots dashboard shows real snapshot history data (SnapshotMonitorService wired)
- `gradle antora` builds without errors
