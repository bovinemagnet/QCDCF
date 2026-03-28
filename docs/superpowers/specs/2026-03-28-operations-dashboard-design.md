# Operations Dashboard Page Design

**Date:** 2026-03-28
**Status:** Approved
**Author:** Paul Snow

## Context

The production hardening work added resilience features (retry, circuit breaker, reconnection), health checks, startup validation, and custom metrics — but none of this is visible in the HTMX dashboard. Operators have no way to see retry counts, circuit breaker state, health check results, or active configuration without checking logs or REST endpoints directly.

## Design

Add a new **Operations** page at `/dashboard/operations` that surfaces all production hardening data in a card-grid layout matching the existing Dashboard page style.

### Page Structure

**Top row: 3 summary cards** (HTMX poll every 5s)

| Card | Data | Colour Logic |
|------|------|-------------|
| Health | ALL UP / DEGRADED / DOWN | Green if source=RUNNING and liveness=UP; yellow if source=STARTING or STOPPED; red if source=FAILED |
| Retries | Total retry count (WAL + sink + snapshot) | Green if 0; yellow if >0; red if >50 |
| Circuit Breaker | CLOSED / OPEN / HALF-OPEN | Green = closed; red = open; yellow = half-open |

**Health status logic:** Derive from `ConnectorService.status()` (which delegates to `ConnectorBootstrap`). `RUNNING` or `SNAPSHOTTING` = UP. `STARTING` or `STOPPED` = DEGRADED. `FAILED` = DOWN. No separate health check beans needed — this uses the same status the existing `SourceHealthCheck` already checks.

**Circuit breaker state logic:** Derive from `ResilientCheckpointRepository` state:
- `circuitOpenedAt == null` → CLOSED
- `circuitOpenedAt != null` AND `now < openedAt + cooldownSeconds` → OPEN
- `circuitOpenedAt != null` AND `now >= openedAt + cooldownSeconds` → HALF-OPEN

**Bottom: 4 detail cards** (2x2 grid)

**Health Checks** (poll 5s):
- Source: connector status + last successful connection timestamp (via `ConnectorService`)
- Sink: type from config (`config.sink().type()`)
- Liveness: connector status != FAILED
- Colour-coded status dots (green/yellow/red)

**Resilience Metrics** (poll 5s):
- WAL reconnect attempts (counter)
- Sink publish retries (counter)
- Snapshot chunk retries (counter)
- Labels with individual values

**Pre-flight Validation** (poll 30s):
- WAL level: logical / other (pass/fail)
- Replication slot: exists + active status (pass/warn)
- Publication: exists (pass/fail)
- Watermark table: exists (pass/warn)
- Re-runs validation on each poll. Acquires a `Connection` from `DataSource` (already injected in `DashboardResource`) with try-with-resources. On connection failure, renders "Database unreachable" state.

**Configuration** (poll 60s):
- WAL retry: `config.resilience().walRetry().delay()`, `maxDelay()`, `jitter()`
- Sink retry: `config.resilience().sinkRetry().maxRetries()`, `delay()`, `timeout()`
- Snapshot retry: `config.resilience().snapshotRetry().maxRetries()`, `delay()`
- Circuit breaker: `config.resilience().checkpointCb().failureThreshold()`, `window()`, `halfOpenDelay()`
- Sustained failure: `config.health().sustainedFailureThreshold()`
- Buffer max: `config.snapshot().maxBufferSize()`

### Fragment Endpoint Paths

| Fragment | Path | Poll |
|----------|------|------|
| Summary cards | `/dashboard/fragments/ops-summary` | 5s |
| Health detail | `/dashboard/fragments/ops-health` | 5s |
| Resilience metrics | `/dashboard/fragments/ops-resilience` | 5s |
| Validation | `/dashboard/fragments/ops-validation` | 30s |
| Configuration | `/dashboard/fragments/ops-config` | 60s |

Note: the summary card combines health, retries, and circuit breaker into a single fragment to reduce requests.

### Files

**New files:**
- `cdc-runtime-quarkus/src/main/resources/templates/operations.html` — main page template
- `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsSummary.html` — top 3 summary cards
- `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsHealth.html` — health checks detail card
- `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsResilience.html` — resilience metrics card
- `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsValidation.html` — pre-flight validation card
- `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsConfig.html` — configuration card

**Modified files:**
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/dashboard/DashboardResource.java` — add Operations page endpoint + 5 fragment endpoints. Inject `ConnectorValidator` and `ConnectorRuntimeConfig` (already available as CDI beans).
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ResilientCheckpointRepository.java` — expose `isCircuitOpen(long cooldownSeconds)`, `consecutiveFailures()` accessors. These are read-only views of existing `AtomicInteger`/`AtomicReference` fields.
- `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java` — add `lastSuccessfulConnection()` delegating to `bootstrap.lastSuccessfulConnection()`
- `cdc-runtime-quarkus/src/main/resources/templates/dashboard.html` — add Operations nav link
- `cdc-runtime-quarkus/src/main/resources/templates/history.html` — add Operations nav link
- `cdc-runtime-quarkus/src/main/resources/templates/snapshots.html` — add Operations nav link
- `cdc-runtime-quarkus/src/main/resources/templates/replication.html` — add Operations nav link

### CDI Wiring for Circuit Breaker State

`ResilientCheckpointRepository` is a plain Java class, not CDI-managed. To expose circuit breaker state to the dashboard without making it CDI:

- `DashboardResource` does NOT inject `ResilientCheckpointRepository` directly
- Instead, `ConnectorBootstrap` (which creates the checkpoint repository) exposes the circuit breaker state via accessor methods: `isCheckpointCircuitOpen()`, `checkpointConsecutiveFailures()`
- These delegate to the `ResilientCheckpointRepository` instance it holds
- `DashboardResource` reads these via `ConnectorService` (its existing facade)

### Data Sources (corrected)

| Card | Service | Method |
|------|---------|--------|
| Health summary | ConnectorService | `status()`, `lastSuccessfulConnection()` (new delegate) |
| Health summary | ConnectorRuntimeConfig | `sink().type()` |
| Retries summary | MetricsService | `walReconnectAttempts()`, `sinkPublishRetries()`, `snapshotChunkRetries()` |
| Circuit breaker summary | ConnectorService | `isCheckpointCircuitOpen()`, `checkpointConsecutiveFailures()` (new delegates) |
| Health detail | ConnectorService | `status()`, `lastSuccessfulConnection()`, `connectorId()` |
| Resilience detail | MetricsService | `walReconnectAttempts()`, `sinkPublishRetries()`, `snapshotChunkRetries()` |
| Validation | ConnectorValidator | individual validate methods with `DataSource.getConnection()` |
| Configuration | ConnectorRuntimeConfig | `resilience().*`, `health().*`, `snapshot().*` |

### Navigation

Add "Operations" link to the nav bar in all 4 existing page templates. Order: Dashboard | History | Snapshots | Replication | **Operations**. Follow existing nav pattern with active-state highlighting. Each page has a back-arrow to Dashboard plus links to other sub-pages.

### HTMX Patterns

Follow the existing fragment pattern used throughout the dashboard:
- Main page loads with `hx-get` attributes pointing to fragment endpoints
- Each fragment has `hx-trigger="load, every Ns"` for polling
- Fragments return HTML partials that replace their container
- Tailwind CSS classes for styling (matching existing dark theme)

## Verification

- All fragment endpoints return valid HTML
- HTMX polling works at specified intervals
- Navigation links work from all pages
- `gradle :cdc-runtime-quarkus:test` passes
- Validation fragment handles database connection failure gracefully
- Visual inspection: cards render correctly with dark theme, colour coding works
