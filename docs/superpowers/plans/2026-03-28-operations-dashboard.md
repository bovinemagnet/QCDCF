# Operations Dashboard Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new Operations page to the HTMX dashboard that surfaces resilience metrics, health checks, circuit breaker state, validation results, and active configuration.

**Architecture:** New `/dashboard/operations` page with 5 HTMX-polled fragment endpoints on DashboardResource. Data from existing CDI beans (MetricsService, ConnectorService, ConnectorValidator, ConnectorRuntimeConfig). Circuit breaker state exposed via ConnectorService delegating to ConnectorBootstrap. All templates follow the existing Tailwind dark-theme card-grid pattern.

**Tech Stack:** Quarkus Qute templates, HTMX polling, Tailwind CSS, Java 25

**Spec:** `docs/superpowers/specs/2026-03-28-operations-dashboard-design.md`

---

## File Structure

### New Files
| File | Responsibility |
|------|---------------|
| `cdc-runtime-quarkus/src/main/resources/templates/operations.html` | Main operations page (nav + HTMX containers) |
| `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsSummary.html` | Top 3 summary cards (health, retries, circuit breaker) |
| `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsHealth.html` | Health checks detail card |
| `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsResilience.html` | Resilience metrics detail card |
| `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsValidation.html` | Pre-flight validation card |
| `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsConfig.html` | Configuration display card |

### Modified Files
| File | Changes |
|------|---------|
| `cdc-runtime-quarkus/.../service/ResilientCheckpointRepository.java` | Add `isCircuitOpen()`, `consecutiveFailures()` accessors |
| `cdc-runtime-quarkus/.../bootstrap/ConnectorBootstrap.java` | Expose circuit breaker state via delegate methods |
| `cdc-runtime-quarkus/.../service/ConnectorService.java` | Add `lastSuccessfulConnection()`, `isCheckpointCircuitOpen()`, `checkpointConsecutiveFailures()` delegates |
| `cdc-runtime-quarkus/.../dashboard/DashboardResource.java` | Add operations page + 5 fragment endpoints, inject ConnectorValidator |
| `cdc-runtime-quarkus/src/main/resources/templates/dashboard.html` | Add Operations nav link |
| `cdc-runtime-quarkus/src/main/resources/templates/history.html` | Add Operations nav link |
| `cdc-runtime-quarkus/src/main/resources/templates/snapshots.html` | Add Operations nav link |
| `cdc-runtime-quarkus/src/main/resources/templates/replication.html` | Add Operations nav link |

---

## Task 1: Expose Circuit Breaker State Through Service Layer

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ResilientCheckpointRepository.java`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java`
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java`

- [ ] **Step 1: Add accessors to ResilientCheckpointRepository**

Read the file first. Then add these public methods:

```java
public int consecutiveFailures() {
    return consecutiveFailures.get();
}

public boolean isCircuitOpen(long cooldownSeconds) {
    Instant openedAt = circuitOpenedAt.get();
    if (openedAt == null) return false;
    return Instant.now().isBefore(openedAt.plusSeconds(cooldownSeconds));
}

public Instant circuitOpenedAt() {
    return circuitOpenedAt.get();
}
```

- [ ] **Step 2: Expose circuit breaker state on ConnectorBootstrap**

Read ConnectorBootstrap. It creates the `ResilientCheckpointRepository` (or may not yet — check). Add a field to hold it and expose delegate methods:

```java
private ResilientCheckpointRepository resilientCheckpoint;

public boolean isCheckpointCircuitOpen() {
    return resilientCheckpoint != null && resilientCheckpoint.isCircuitOpen(30);
}

public int checkpointConsecutiveFailures() {
    return resilientCheckpoint != null ? resilientCheckpoint.consecutiveFailures() : 0;
}
```

- [ ] **Step 3: Add delegates to ConnectorService**

```java
public Instant lastSuccessfulConnection() {
    return bootstrap.lastSuccessfulConnection();
}

public boolean isCheckpointCircuitOpen() {
    return bootstrap.isCheckpointCircuitOpen();
}

public int checkpointConsecutiveFailures() {
    return bootstrap.checkpointConsecutiveFailures();
}
```

- [ ] **Step 4: Compile and verify**

Run: `gradle :cdc-runtime-quarkus:compileJava`

- [ ] **Step 5: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ResilientCheckpointRepository.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/bootstrap/ConnectorBootstrap.java \
       cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/service/ConnectorService.java
git commit -m "feat: expose circuit breaker state through service layer for dashboard"
```

---

## Task 2: Add Navigation Links to All Existing Pages

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/resources/templates/dashboard.html`
- Modify: `cdc-runtime-quarkus/src/main/resources/templates/history.html`
- Modify: `cdc-runtime-quarkus/src/main/resources/templates/snapshots.html`
- Modify: `cdc-runtime-quarkus/src/main/resources/templates/replication.html`

- [ ] **Step 1: Add Operations link to dashboard.html nav**

In the nav `<div class="flex items-center gap-4">`, add before the closing `<span>`:
```html
<a href="/dashboard/operations" class="text-sm text-gray-400 hover:text-indigo-400">Operations &rarr;</a>
```

- [ ] **Step 2: Add Operations link to replication.html nav**

Add after the History link:
```html
<a href="/dashboard/operations" class="text-sm text-gray-400 hover:text-indigo-400">Operations &rarr;</a>
```

- [ ] **Step 3: Add Operations link to history.html and snapshots.html**

Same pattern — read each file first, add the Operations link in the nav div.

- [ ] **Step 4: Commit**

```bash
git add cdc-runtime-quarkus/src/main/resources/templates/dashboard.html \
       cdc-runtime-quarkus/src/main/resources/templates/history.html \
       cdc-runtime-quarkus/src/main/resources/templates/snapshots.html \
       cdc-runtime-quarkus/src/main/resources/templates/replication.html
git commit -m "feat: add Operations nav link to all dashboard pages"
```

---

## Task 3: Create Operations Page Template and Summary Fragment

**Files:**
- Create: `cdc-runtime-quarkus/src/main/resources/templates/operations.html`
- Create: `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsSummary.html`

- [ ] **Step 1: Create operations.html main page**

Follow the exact pattern from `replication.html` (same head, nav, body structure):

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>QCDCF — Operations</title>
    <script src="https://unpkg.com/htmx.org@2.0.4"></script>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 text-gray-100 min-h-screen">
    <nav class="bg-gray-800 border-b border-gray-700 px-6 py-4">
        <div class="flex items-center justify-between max-w-7xl mx-auto">
            <h1 class="text-xl font-bold text-indigo-400">Operations</h1>
            <div class="flex items-center gap-4">
                <a href="/dashboard" class="text-sm text-gray-400 hover:text-indigo-400">&larr; Dashboard</a>
                <a href="/dashboard/replication" class="text-sm text-gray-400 hover:text-indigo-400">Replication Health &rarr;</a>
                <a href="/dashboard/snapshots" class="text-sm text-gray-400 hover:text-indigo-400">Snapshots &rarr;</a>
                <a href="/dashboard/history" class="text-sm text-gray-400 hover:text-indigo-400">History &rarr;</a>
            </div>
        </div>
    </nav>

    <main class="max-w-7xl mx-auto px-6 py-8 space-y-6">

        <!-- Top Row: Summary Cards -->
        <div hx-get="/dashboard/fragments/ops-summary"
             hx-trigger="every 5s"
             hx-swap="innerHTML">
            {#include fragments/operationsSummary /}
        </div>

        <!-- Detail Cards: 2x2 grid -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">

            <!-- Health Checks -->
            <div class="bg-gray-800 rounded-lg border border-gray-700 p-6"
                 hx-get="/dashboard/fragments/ops-health"
                 hx-trigger="every 5s"
                 hx-swap="innerHTML">
                {#include fragments/operationsHealth /}
            </div>

            <!-- Resilience Metrics -->
            <div class="bg-gray-800 rounded-lg border border-gray-700 p-6"
                 hx-get="/dashboard/fragments/ops-resilience"
                 hx-trigger="every 5s"
                 hx-swap="innerHTML">
                {#include fragments/operationsResilience /}
            </div>

            <!-- Pre-flight Validation -->
            <div class="bg-gray-800 rounded-lg border border-gray-700 p-6"
                 hx-get="/dashboard/fragments/ops-validation"
                 hx-trigger="every 30s"
                 hx-swap="innerHTML">
                {#include fragments/operationsValidation /}
            </div>

            <!-- Configuration -->
            <div class="bg-gray-800 rounded-lg border border-gray-700 p-6"
                 hx-get="/dashboard/fragments/ops-config"
                 hx-trigger="every 60s"
                 hx-swap="innerHTML">
                {#include fragments/operationsConfig /}
            </div>

        </div>

    </main>
</body>
</html>
```

- [ ] **Step 2: Create operationsSummary.html fragment**

3-column card grid matching `metricsCards.html` pattern:

```html
<div class="grid grid-cols-3 gap-4">
    <!-- Health -->
    <div class="bg-gray-800 rounded-lg border border-gray-700 p-4 text-centre">
        <p class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-1">Health</p>
        <p class="text-2xl font-bold font-mono {healthColour}">{healthStatus}</p>
    </div>
    <!-- Retries -->
    <div class="bg-gray-800 rounded-lg border border-gray-700 p-4 text-centre">
        <p class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-1">Total Retries</p>
        <p class="text-3xl font-bold font-mono {retriesColour}">{totalRetries}</p>
    </div>
    <!-- Circuit Breaker -->
    <div class="bg-gray-800 rounded-lg border border-gray-700 p-4 text-centre">
        <p class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-1">Circuit Breaker</p>
        <p class="text-2xl font-bold font-mono {cbColour}">{cbStatus}</p>
    </div>
</div>
```

- [ ] **Step 3: Commit**

```bash
git add cdc-runtime-quarkus/src/main/resources/templates/operations.html \
       cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsSummary.html
git commit -m "feat: create operations page template and summary fragment"
```

---

## Task 4: Create Detail Fragment Templates

**Files:**
- Create: `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsHealth.html`
- Create: `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsResilience.html`
- Create: `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsValidation.html`
- Create: `cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsConfig.html`

- [ ] **Step 1: Create operationsHealth.html**

```html
<h2 class="text-sm font-medium text-gray-400 uppercase tracking-wider mb-4">Health Checks</h2>
<div class="space-y-3">
    <div class="flex justify-between items-centre">
        <span class="text-gray-400">Source</span>
        <span class="flex items-centre gap-2">
            <span class="inline-block w-2 h-2 rounded-full {sourceColour}"></span>
            <span class="font-mono text-sm {sourceColour}">{sourceStatus}</span>
        </span>
    </div>
    <div class="flex justify-between items-centre">
        <span class="text-gray-400">Last Connection</span>
        <span class="font-mono text-sm text-gray-300">{lastConnection}</span>
    </div>
    <div class="flex justify-between items-centre">
        <span class="text-gray-400">Sink</span>
        <span class="flex items-centre gap-2">
            <span class="inline-block w-2 h-2 rounded-full bg-emerald-400"></span>
            <span class="font-mono text-sm text-gray-300">{sinkType}</span>
        </span>
    </div>
    <div class="flex justify-between items-centre">
        <span class="text-gray-400">Liveness</span>
        <span class="flex items-centre gap-2">
            <span class="inline-block w-2 h-2 rounded-full {livenessColour}"></span>
            <span class="font-mono text-sm {livenessColour}">{livenessStatus}</span>
        </span>
    </div>
</div>
```

- [ ] **Step 2: Create operationsResilience.html**

```html
<h2 class="text-sm font-medium text-gray-400 uppercase tracking-wider mb-4">Resilience Metrics</h2>
<div class="space-y-3">
    <div class="flex justify-between">
        <span class="text-gray-400">WAL Reconnect Attempts</span>
        <span class="font-mono text-lg text-indigo-400">{walReconnects}</span>
    </div>
    <div class="flex justify-between">
        <span class="text-gray-400">Sink Publish Retries</span>
        <span class="font-mono text-lg text-indigo-400">{sinkRetries}</span>
    </div>
    <div class="flex justify-between">
        <span class="text-gray-400">Snapshot Chunk Retries</span>
        <span class="font-mono text-lg text-indigo-400">{snapshotRetries}</span>
    </div>
    <div class="flex justify-between">
        <span class="text-gray-400">Checkpoint CB Failures</span>
        <span class="font-mono text-lg {cbFailColour}">{cbFailures}</span>
    </div>
</div>
```

- [ ] **Step 3: Create operationsValidation.html**

```html
<h2 class="text-sm font-medium text-gray-400 uppercase tracking-wider mb-4">Pre-flight Validation</h2>
{#if validationError}
<p class="text-sm text-red-400 italic">{validationError}</p>
{#else}
<div class="space-y-2">
    {#for check in checks}
    <div class="flex justify-between items-centre">
        <span class="text-gray-400 text-sm">{check.name}</span>
        <span class="font-mono text-sm {#if check.status == 'OK'}text-emerald-400{#else if check.status == 'WARN'}text-yellow-400{#else}text-red-400{/if}">{check.status}</span>
    </div>
    {/for}
</div>
{/if}
```

- [ ] **Step 4: Create operationsConfig.html**

```html
<h2 class="text-sm font-medium text-gray-400 uppercase tracking-wider mb-4">Active Configuration</h2>
<div class="space-y-2 text-sm">
    <div class="text-xs font-medium text-gray-500 uppercase mt-2">WAL Retry</div>
    <div class="flex justify-between"><span class="text-gray-400">Delay</span><span class="font-mono text-gray-300">{walRetryDelay}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Max Delay</span><span class="font-mono text-gray-300">{walRetryMaxDelay}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Jitter</span><span class="font-mono text-gray-300">{walRetryJitter}</span></div>

    <div class="text-xs font-medium text-gray-500 uppercase mt-3">Sink Retry</div>
    <div class="flex justify-between"><span class="text-gray-400">Max Retries</span><span class="font-mono text-gray-300">{sinkMaxRetries}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Delay</span><span class="font-mono text-gray-300">{sinkDelay}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Timeout</span><span class="font-mono text-gray-300">{sinkTimeout}</span></div>

    <div class="text-xs font-medium text-gray-500 uppercase mt-3">Snapshot Retry</div>
    <div class="flex justify-between"><span class="text-gray-400">Max Retries</span><span class="font-mono text-gray-300">{snapshotMaxRetries}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Delay</span><span class="font-mono text-gray-300">{snapshotDelay}</span></div>

    <div class="text-xs font-medium text-gray-500 uppercase mt-3">Circuit Breaker</div>
    <div class="flex justify-between"><span class="text-gray-400">Failure Threshold</span><span class="font-mono text-gray-300">{cbThreshold}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Window</span><span class="font-mono text-gray-300">{cbWindow}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Half-Open Delay</span><span class="font-mono text-gray-300">{cbHalfOpenDelay}</span></div>

    <div class="text-xs font-medium text-gray-500 uppercase mt-3">Thresholds</div>
    <div class="flex justify-between"><span class="text-gray-400">Sustained Failure</span><span class="font-mono text-gray-300">{sustainedFailureThreshold}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Buffer Max Size</span><span class="font-mono text-gray-300">{bufferMaxSize}</span></div>
</div>
```

- [ ] **Step 5: Commit**

```bash
git add cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsHealth.html \
       cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsResilience.html \
       cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsValidation.html \
       cdc-runtime-quarkus/src/main/resources/templates/fragments/operationsConfig.html
git commit -m "feat: create operations detail fragment templates"
```

---

## Task 5: Wire DashboardResource Endpoints

**Files:**
- Modify: `cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/dashboard/DashboardResource.java`

- [ ] **Step 1: Read DashboardResource.java fully to understand current structure**

- [ ] **Step 2: Add template injections**

Add these `@Inject` fields near the existing template fields:

```java
@Inject
Template operations;

@Inject
@io.quarkus.qute.Location("fragments/operationsSummary")
Template operationsSummaryFragment;

@Inject
@io.quarkus.qute.Location("fragments/operationsHealth")
Template operationsHealthFragment;

@Inject
@io.quarkus.qute.Location("fragments/operationsResilience")
Template operationsResilienceFragment;

@Inject
@io.quarkus.qute.Location("fragments/operationsValidation")
Template operationsValidationFragment;

@Inject
@io.quarkus.qute.Location("fragments/operationsConfig")
Template operationsConfigFragment;
```

- [ ] **Step 3: Add service injections**

Add if not already present:
```java
@Inject
com.paulsnow.qcdcf.runtime.service.ConnectorValidator validator;
```

(`ConnectorService`, `MetricsService`, `ConnectorRuntimeConfig`, and `DataSource` are already injected.)

- [ ] **Step 4: Add the operations page endpoint**

```java
@GET
@Path("/operations")
@Produces(MediaType.TEXT_HTML)
public TemplateInstance operationsPage() {
    return buildOperationsData(operations);
}
```

- [ ] **Step 5: Add the 5 fragment endpoints**

```java
@GET
@Path("/fragments/ops-summary")
@Produces(MediaType.TEXT_HTML)
public TemplateInstance opsSummaryFragment() {
    return buildOpsSummaryData(operationsSummaryFragment);
}

@GET
@Path("/fragments/ops-health")
@Produces(MediaType.TEXT_HTML)
public TemplateInstance opsHealthFragment() {
    return buildOpsHealthData(operationsHealthFragment);
}

@GET
@Path("/fragments/ops-resilience")
@Produces(MediaType.TEXT_HTML)
public TemplateInstance opsResilienceFragment() {
    return buildOpsResilienceData(operationsResilienceFragment);
}

@GET
@Path("/fragments/ops-validation")
@Produces(MediaType.TEXT_HTML)
public TemplateInstance opsValidationFragment() {
    return buildOpsValidationData(operationsValidationFragment);
}

@GET
@Path("/fragments/ops-config")
@Produces(MediaType.TEXT_HTML)
public TemplateInstance opsConfigFragment() {
    return buildOpsConfigData(operationsConfigFragment);
}
```

- [ ] **Step 6: Add the private builder methods**

```java
private TemplateInstance buildOperationsData(Template template) {
    return buildOpsSummaryData(template);
}

private TemplateInstance buildOpsSummaryData(Template template) {
    String status = connectorService.status().name();
    boolean isFailed = "FAILED".equals(status);
    boolean isRunning = "RUNNING".equals(status) || "SNAPSHOTTING".equals(status);
    String healthStatus = isFailed ? "DOWN" : isRunning ? "ALL UP" : "DEGRADED";
    String healthColour = isFailed ? "text-red-400" : isRunning ? "text-emerald-400" : "text-yellow-400";

    long totalRetries = metricsService.walReconnectAttempts()
            + metricsService.sinkPublishRetries()
            + metricsService.snapshotChunkRetries();
    String retriesColour = totalRetries == 0 ? "text-emerald-400"
            : totalRetries > 50 ? "text-red-400" : "text-yellow-400";

    boolean cbOpen = connectorService.isCheckpointCircuitOpen();
    String cbStatus = cbOpen ? "OPEN" : "CLOSED";
    String cbColour = cbOpen ? "text-red-400" : "text-emerald-400";

    return template
            .data("healthStatus", healthStatus)
            .data("healthColour", healthColour)
            .data("totalRetries", totalRetries)
            .data("retriesColour", retriesColour)
            .data("cbStatus", cbStatus)
            .data("cbColour", cbColour);
}

private TemplateInstance buildOpsHealthData(Template template) {
    String status = connectorService.status().name();
    boolean isRunning = "RUNNING".equals(status) || "SNAPSHOTTING".equals(status);
    boolean isFailed = "FAILED".equals(status);
    String sourceColour = isFailed ? "text-red-400" : isRunning ? "text-emerald-400" : "text-yellow-400";

    Instant lastConn = connectorService.lastSuccessfulConnection();
    String lastConnection = lastConn != null ? lastConn.toString() : "never";

    boolean livenessUp = !isFailed;
    String livenessColour = livenessUp ? "text-emerald-400" : "text-red-400";

    return template
            .data("sourceStatus", status)
            .data("sourceColour", sourceColour)
            .data("lastConnection", lastConnection)
            .data("sinkType", config.sink().type())
            .data("livenessStatus", livenessUp ? "UP" : "DOWN")
            .data("livenessColour", livenessColour);
}

private TemplateInstance buildOpsResilienceData(Template template) {
    int cbFailures = connectorService.checkpointConsecutiveFailures();
    String cbFailColour = cbFailures > 0 ? "text-yellow-400" : "text-emerald-400";

    return template
            .data("walReconnects", metricsService.walReconnectAttempts())
            .data("sinkRetries", metricsService.sinkPublishRetries())
            .data("snapshotRetries", metricsService.snapshotChunkRetries())
            .data("cbFailures", cbFailures)
            .data("cbFailColour", cbFailColour);
}

private TemplateInstance buildOpsValidationData(Template template) {
    var checks = new java.util.ArrayList<java.util.Map<String, String>>();
    String validationError = null;

    try (Connection conn = dataSource.getConnection()) {
        for (var result : validator.validateWalLevel(conn)) {
            checks.add(Map.of("name", result.get("check"), "status", result.get("status")));
        }
        for (var result : validator.validateSlot(conn, config.source().slotName())) {
            checks.add(Map.of("name", result.get("check"), "status", result.get("status")));
        }
        for (var result : validator.validatePublication(conn, config.source().publicationName())) {
            checks.add(Map.of("name", result.get("check"), "status", result.get("status")));
        }
        for (var result : validator.validateWatermarkTable(conn)) {
            checks.add(Map.of("name", result.get("check"), "status", result.get("status")));
        }
    } catch (Exception e) {
        validationError = "Database unreachable: " + e.getMessage();
        LOG.warnf("Validation fragment failed: %s", e.getMessage());
    }

    return template
            .data("checks", checks)
            .data("validationError", validationError);
}

private TemplateInstance buildOpsConfigData(Template template) {
    var r = config.resilience();
    var h = config.health();
    return template
            .data("walRetryDelay", r.walRetry().delay())
            .data("walRetryMaxDelay", r.walRetry().maxDelay())
            .data("walRetryJitter", r.walRetry().jitter())
            .data("sinkMaxRetries", r.sinkRetry().maxRetries())
            .data("sinkDelay", r.sinkRetry().delay())
            .data("sinkTimeout", r.sinkRetry().timeout())
            .data("snapshotMaxRetries", r.snapshotRetry().maxRetries())
            .data("snapshotDelay", r.snapshotRetry().delay())
            .data("cbThreshold", r.checkpointCb().failureThreshold())
            .data("cbWindow", r.checkpointCb().window())
            .data("cbHalfOpenDelay", r.checkpointCb().halfOpenDelay())
            .data("sustainedFailureThreshold", h.sustainedFailureThreshold())
            .data("bufferMaxSize", config.snapshot().maxBufferSize());
}
```

- [ ] **Step 7: Compile and run tests**

Run: `gradle :cdc-runtime-quarkus:compileJava`
Run: `gradle :cdc-runtime-quarkus:test`

- [ ] **Step 8: Commit**

```bash
git add cdc-runtime-quarkus/src/main/java/com/paulsnow/qcdcf/runtime/dashboard/DashboardResource.java
git commit -m "feat: wire operations page and fragment endpoints in DashboardResource"
```

---

## Task 6: Final Verification

- [ ] **Step 1: Run all tests**

Run: `gradle :cdc-api-model:test :cdc-core:test :cdc-runtime-quarkus:test`

- [ ] **Step 2: Verify Antora docs still build**

Run: `gradle antora`

- [ ] **Step 3: Commit any fixes**

If anything failed, fix and commit.
