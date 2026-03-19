# Project Skeleton for the PostgreSQL CDC MVP

## Purpose

This document provides the initial project skeleton for the MVP.

It is meant to answer three practical questions:

1. what modules should exist,
2. what packages should be created first,
3. what classes and interfaces should be written first.

This is not a full architecture document. It is a coding starter map.

The goal is to help implementation begin quickly while keeping the codebase aligned with the PRD, technical architecture spec, MVP strategy, and implementation plan.

---

## Design intent

The skeleton should support these principles:

* PostgreSQL first
* plain Java core
* Quarkus at the runtime shell
* internal messaging for the MVP pipeline
* pluggable sink boundary
* minimal abstraction until the first working flow is proven

That means the project should feel small and direct at the beginning.

---

## Top-level module layout

Start with a small multi-module Gradle project.

```text
cdc-platform/
  settings.gradle
  build.gradle
  gradle.properties

  cdc-api-model/
  cdc-core/
  cdc-postgres/
  cdc-runtime-quarkus/
  cdc-test-kit/
```

### Why only these modules first

This layout is enough to separate:

* shared domain types,
* core CDC logic,
* PostgreSQL-specific behavior,
* runtime wiring,
* test support.

Do not add separate Kafka or metadata modules until they are truly needed.

For the MVP, keep those inside runtime or core if that reduces ceremony.

---

## Root Gradle structure

### `settings.gradle`

```groovy
rootProject.name = 'cdc-platform'

include 'cdc-api-model'
include 'cdc-core'
include 'cdc-postgres'
include 'cdc-runtime-quarkus'
include 'cdc-test-kit'
```

### Root `build.gradle`

```groovy
plugins {
    id 'java'
}

allprojects {
    group = 'com.example.cdc'
    version = '0.1.0-SNAPSHOT'

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply plugin: 'java'

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    test {
        useJUnitPlatform()
    }
}
```

### `gradle.properties`

Keep only a few properties first:

```properties
quarkusPlatformGroupId=io.quarkus.platform
quarkusPlatformArtifactId=quarkus-bom
quarkusPlatformVersion=<current-version>
```

Do not over-engineer the build in the first pass.

---

## Module skeletons

## 1. `cdc-api-model`

### Purpose

Stable shared model types.

This module should contain no Quarkus annotations and no persistence framework dependencies.

### Suggested package structure

```text
cdc-api-model/
  src/main/java/com/example/cdc/model/
    ChangeEnvelope.java
    TableId.java
    RowKey.java
    SourcePosition.java
    WatermarkContext.java
    OperationType.java
    CaptureMode.java
```

### First classes

#### `OperationType.java`

```java
package com.example.cdc.model;

public enum OperationType {
    INSERT,
    UPDATE,
    DELETE,
    SNAPSHOT,
    WATERMARK
}
```

#### `CaptureMode.java`

```java
package com.example.cdc.model;

public enum CaptureMode {
    LOG,
    SNAPSHOT,
    CONTROL
}
```

#### `TableId.java`

```java
package com.example.cdc.model;

public record TableId(String schemaName, String tableName) {
    public String canonicalName() {
        return schemaName + "." + tableName;
    }
}
```

#### `RowKey.java`

```java
package com.example.cdc.model;

import java.util.Map;

public record RowKey(TableId tableId, Map<String, Object> keyValues) {
}
```

#### `SourcePosition.java`

```java
package com.example.cdc.model;

import java.time.Instant;

public record SourcePosition(
        String sourceType,
        String databaseName,
        long lsn,
        Long txId,
        Instant commitTimestamp) {
}
```

#### `WatermarkContext.java`

```java
package com.example.cdc.model;

import java.util.UUID;

public record WatermarkContext(
        UUID windowId,
        UUID lowWatermarkId,
        UUID highWatermarkId) {
}
```

#### `ChangeEnvelope.java`

```java
package com.example.cdc.model;

import java.util.Map;
import java.util.UUID;

public record ChangeEnvelope(
        UUID eventId,
        String connectorId,
        TableId tableId,
        RowKey rowKey,
        OperationType operationType,
        CaptureMode captureMode,
        Map<String, Object> before,
        Map<String, Object> after,
        SourcePosition sourcePosition,
        WatermarkContext watermarkContext) {
}
```

### Notes

Keep this module very stable.

Most later change should happen around it, not inside it.

---

## 2. `cdc-core`

### Purpose

Plain Java implementation of core CDC behavior.

This is where the watermark logic, reconciliation, snapshot planning, checkpoints, and sink abstraction should live.

### Suggested package structure

```text
cdc-core/
  src/main/java/com/example/cdc/core/
    sink/
    snapshot/
    watermark/
    reconcile/
    checkpoint/
    connector/
```

### First package and class layout

```text
com/example/cdc/core/sink/
  EventSink.java
  LoggingEventSink.java
  InMemoryEventSink.java

com/example/cdc/core/snapshot/
  SnapshotOptions.java
  SnapshotChunkPlan.java
  SnapshotChunkResult.java
  ChunkPlanner.java
  DefaultChunkPlanner.java
  SnapshotCoordinator.java

com/example/cdc/core/watermark/
  WatermarkWindow.java
  WatermarkCoordinator.java
  DefaultWatermarkCoordinator.java

com/example/cdc/core/reconcile/
  ReconciliationEngine.java
  DefaultReconciliationEngine.java
  ReconciliationResult.java

com/example/cdc/core/checkpoint/
  ConnectorCheckpoint.java
  CheckpointManager.java

com/example/cdc/core/connector/
  ConnectorSupervisor.java
  ConnectorStatus.java
```

### First interfaces and classes

#### `EventSink.java`

```java
package com.example.cdc.core.sink;

import com.example.cdc.model.ChangeEnvelope;
import java.util.List;

public interface EventSink {
    void publish(ChangeEnvelope event);
    void publishBatch(List<ChangeEnvelope> events);
}
```

#### `LoggingEventSink.java`

```java
package com.example.cdc.core.sink;

import com.example.cdc.model.ChangeEnvelope;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingEventSink implements EventSink {
    private static final Logger log = LoggerFactory.getLogger(LoggingEventSink.class);

    @Override
    public void publish(ChangeEnvelope event) {
        log.info("CDC event: {}", event);
    }

    @Override
    public void publishBatch(List<ChangeEnvelope> events) {
        events.forEach(this::publish);
    }
}
```

#### `SnapshotOptions.java`

```java
package com.example.cdc.core.snapshot;

public record SnapshotOptions(int chunkSize) {
}
```

#### `SnapshotChunkPlan.java`

```java
package com.example.cdc.core.snapshot;

import com.example.cdc.model.TableId;
import java.util.Map;
import java.util.UUID;

public record SnapshotChunkPlan(
        UUID jobId,
        UUID chunkId,
        TableId tableId,
        Map<String, Object> startKeyExclusive,
        int chunkSize) {
}
```

#### `SnapshotChunkResult.java`

```java
package com.example.cdc.core.snapshot;

import com.example.cdc.model.ChangeEnvelope;
import java.util.List;
import java.util.Map;

public record SnapshotChunkResult(
        List<ChangeEnvelope> rows,
        Map<String, Object> lastKey,
        boolean hasMore) {
}
```

#### `ChunkPlanner.java`

```java
package com.example.cdc.core.snapshot;

import com.example.cdc.model.TableId;
import java.util.Map;

public interface ChunkPlanner {
    SnapshotChunkPlan firstChunk(TableId tableId, SnapshotOptions options);
    SnapshotChunkPlan nextChunk(SnapshotChunkPlan previousPlan, Map<String, Object> lastKey);
}
```

#### `DefaultChunkPlanner.java`

```java
package com.example.cdc.core.snapshot;

import com.example.cdc.model.TableId;
import java.util.Map;
import java.util.UUID;

public class DefaultChunkPlanner implements ChunkPlanner {
    @Override
    public SnapshotChunkPlan firstChunk(TableId tableId, SnapshotOptions options) {
        return new SnapshotChunkPlan(UUID.randomUUID(), UUID.randomUUID(), tableId, null, options.chunkSize());
    }

    @Override
    public SnapshotChunkPlan nextChunk(SnapshotChunkPlan previousPlan, Map<String, Object> lastKey) {
        return new SnapshotChunkPlan(
                previousPlan.jobId(),
                UUID.randomUUID(),
                previousPlan.tableId(),
                lastKey,
                previousPlan.chunkSize());
    }
}
```

#### `WatermarkWindow.java`

```java
package com.example.cdc.core.watermark;

import com.example.cdc.model.TableId;
import java.util.UUID;

public record WatermarkWindow(
        UUID windowId,
        TableId tableId,
        UUID lowWatermarkId,
        UUID highWatermarkId) {
}
```

#### `ReconciliationEngine.java`

```java
package com.example.cdc.core.reconcile;

import com.example.cdc.core.snapshot.SnapshotChunkResult;
import com.example.cdc.core.watermark.WatermarkWindow;
import com.example.cdc.model.ChangeEnvelope;
import java.util.List;

public interface ReconciliationEngine {
    ReconciliationResult reconcile(
            WatermarkWindow window,
            SnapshotChunkResult chunk,
            List<ChangeEnvelope> logEventsInWindow);
}
```

#### `ReconciliationResult.java`

```java
package com.example.cdc.core.reconcile;

import com.example.cdc.model.ChangeEnvelope;
import java.util.List;

public record ReconciliationResult(
        List<ChangeEnvelope> orderedLogEvents,
        List<ChangeEnvelope> survivingSnapshotRows) {
}
```

#### `DefaultReconciliationEngine.java`

```java
package com.example.cdc.core.reconcile;

import com.example.cdc.core.snapshot.SnapshotChunkResult;
import com.example.cdc.core.watermark.WatermarkWindow;
import com.example.cdc.model.ChangeEnvelope;
import com.example.cdc.model.OperationType;
import com.example.cdc.model.RowKey;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DefaultReconciliationEngine implements ReconciliationEngine {
    @Override
    public ReconciliationResult reconcile(
            WatermarkWindow window,
            SnapshotChunkResult chunk,
            List<ChangeEnvelope> logEventsInWindow) {

        Map<RowKey, ChangeEnvelope> remainingSnapshotRows = new LinkedHashMap<>();
        for (ChangeEnvelope row : chunk.rows()) {
            remainingSnapshotRows.put(row.rowKey(), row);
        }

        for (ChangeEnvelope event : logEventsInWindow) {
            if (event.operationType() != OperationType.WATERMARK) {
                remainingSnapshotRows.remove(event.rowKey());
            }
        }

        return new ReconciliationResult(
                logEventsInWindow,
                new ArrayList<>(remainingSnapshotRows.values()));
    }
}
```

### Notes

This module should be where most of the real unit testing happens first.

---

## 3. `cdc-postgres`

### Purpose

PostgreSQL-specific source behavior.

This includes:

* metadata queries,
* logical replication,
* watermark writes,
* chunk SQL generation,
* snapshot reads.

### Suggested package structure

```text
cdc-postgres/
  src/main/java/com/example/cdc/postgres/
    metadata/
    replication/
    snapshot/
    watermark/
    sql/
```

### Package and class layout

```text
com/example/cdc/postgres/metadata/
  TableMetadata.java
  ColumnMetadata.java
  PostgresTableMetadataReader.java
  ReplicaIdentityValidator.java

com/example/cdc/postgres/replication/
  PostgresLogicalReplicationClient.java
  PostgresLogStreamReader.java
  RawReplicationMessage.java
  PgOutputMessageDecoder.java
  PgOutputEventNormalizer.java

com/example/cdc/postgres/snapshot/
  PostgresSnapshotReader.java

com/example/cdc/postgres/watermark/
  PostgresWatermarkWriter.java
  WatermarkBoundary.java

com/example/cdc/postgres/sql/
  PostgresChunkSqlBuilder.java
```

### First metadata classes

#### `ColumnMetadata.java`

```java
package com.example.cdc.postgres.metadata;

public record ColumnMetadata(
        String columnName,
        String dataType,
        int ordinalPosition,
        boolean primaryKey) {
}
```

#### `TableMetadata.java`

```java
package com.example.cdc.postgres.metadata;

import com.example.cdc.model.TableId;
import java.util.List;

public record TableMetadata(
        TableId tableId,
        List<ColumnMetadata> columns,
        List<String> primaryKeyColumns,
        String replicaIdentity) {
}
```

#### `PostgresTableMetadataReader.java`

```java
package com.example.cdc.postgres.metadata;

import com.example.cdc.model.TableId;
import javax.sql.DataSource;

public class PostgresTableMetadataReader {
    private final DataSource dataSource;

    public PostgresTableMetadataReader(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public TableMetadata read(TableId tableId) {
        throw new UnsupportedOperationException("Implement metadata query");
    }
}
```

### First replication classes

#### `RawReplicationMessage.java`

```java
package com.example.cdc.postgres.replication;

import java.nio.ByteBuffer;

public record RawReplicationMessage(long lsn, ByteBuffer payload) {
}
```

#### `PostgresLogicalReplicationClient.java`

```java
package com.example.cdc.postgres.replication;

public interface PostgresLogicalReplicationClient {
    void start(RawReplicationMessageHandler handler);
    void stop();
}
```

#### `RawReplicationMessageHandler.java`

```java
package com.example.cdc.postgres.replication;

@FunctionalInterface
public interface RawReplicationMessageHandler {
    void onMessage(RawReplicationMessage message);
}
```

#### `PgOutputMessageDecoder.java`

```java
package com.example.cdc.postgres.replication;

public interface PgOutputMessageDecoder {
    DecodedReplicationMessage decode(RawReplicationMessage message);
}
```

#### `DecodedReplicationMessage.java`

```java
package com.example.cdc.postgres.replication;

import java.time.Instant;
import java.util.Map;

public record DecodedReplicationMessage(
        String schemaName,
        String tableName,
        String operation,
        Map<String, Object> oldValues,
        Map<String, Object> newValues,
        Long txId,
        Instant commitTimestamp,
        long lsn) {
}
```

#### `PgOutputEventNormalizer.java`

```java
package com.example.cdc.postgres.replication;

import com.example.cdc.model.ChangeEnvelope;

public interface PgOutputEventNormalizer {
    ChangeEnvelope normalize(String connectorId, DecodedReplicationMessage message);
}
```

#### `PostgresLogStreamReader.java`

```java
package com.example.cdc.postgres.replication;

import com.example.cdc.model.ChangeEnvelope;
import java.util.function.Consumer;

public class PostgresLogStreamReader {
    private final PostgresLogicalReplicationClient client;
    private final PgOutputMessageDecoder decoder;
    private final PgOutputEventNormalizer normalizer;
    private final String connectorId;

    public PostgresLogStreamReader(
            PostgresLogicalReplicationClient client,
            PgOutputMessageDecoder decoder,
            PgOutputEventNormalizer normalizer,
            String connectorId) {
        this.client = client;
        this.decoder = decoder;
        this.normalizer = normalizer;
        this.connectorId = connectorId;
    }

    public void start(Consumer<ChangeEnvelope> consumer) {
        client.start(raw -> {
            DecodedReplicationMessage decoded = decoder.decode(raw);
            ChangeEnvelope envelope = normalizer.normalize(connectorId, decoded);
            consumer.accept(envelope);
        });
    }

    public void stop() {
        client.stop();
    }
}
```

### First snapshot and watermark classes

#### `WatermarkBoundary.java`

```java
package com.example.cdc.postgres.watermark;

import java.util.UUID;

public record WatermarkBoundary(UUID watermarkId, long lsn) {
}
```

#### `PostgresWatermarkWriter.java`

```java
package com.example.cdc.postgres.watermark;

import java.util.UUID;
import javax.sql.DataSource;

public class PostgresWatermarkWriter {
    private final DataSource dataSource;

    public PostgresWatermarkWriter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public UUID writeLowWatermark() {
        throw new UnsupportedOperationException("Implement low watermark write");
    }

    public UUID writeHighWatermark() {
        throw new UnsupportedOperationException("Implement high watermark write");
    }
}
```

#### `PostgresChunkSqlBuilder.java`

```java
package com.example.cdc.postgres.sql;

import com.example.cdc.postgres.metadata.TableMetadata;
import java.util.Map;

public class PostgresChunkSqlBuilder {
    public String buildChunkSql(TableMetadata metadata, Map<String, Object> startKeyExclusive, int chunkSize) {
        throw new UnsupportedOperationException("Implement keyset SQL builder");
    }
}
```

#### `PostgresSnapshotReader.java`

```java
package com.example.cdc.postgres.snapshot;

import com.example.cdc.core.snapshot.SnapshotChunkPlan;
import com.example.cdc.core.snapshot.SnapshotChunkResult;
import javax.sql.DataSource;

public class PostgresSnapshotReader {
    private final DataSource dataSource;

    public PostgresSnapshotReader(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public SnapshotChunkResult readChunk(SnapshotChunkPlan plan) {
        throw new UnsupportedOperationException("Implement chunk read");
    }
}
```

### Notes

Keep the PostgreSQL module very focused.

Do not mix Quarkus REST, resource classes, or CDI-heavy application logic into this module.

---

## 4. `cdc-runtime-quarkus`

### Purpose

Runtime shell, Quarkus wiring, REST endpoints, internal messaging channels, configuration, and health.

### Suggested package structure

```text
cdc-runtime-quarkus/
  src/main/java/com/example/cdc/runtime/
    config/
    messaging/
    resource/
    service/
    health/
    bootstrap/
```

### Runtime package and class layout

```text
com/example/cdc/runtime/config/
  ConnectorRuntimeConfig.java

com/example/cdc/runtime/bootstrap/
  ConnectorBootstrap.java

com/example/cdc/runtime/messaging/
  WalIngressChannelBridge.java
  PublishRequestProcessor.java

com/example/cdc/runtime/service/
  ConnectorService.java
  SnapshotService.java

com/example/cdc/runtime/resource/
  ConnectorResource.java

com/example/cdc/runtime/health/
  SourceHealthCheck.java
  RuntimeHealthCheck.java
```

### Runtime build file

`cdc-runtime-quarkus/build.gradle`

```groovy
plugins {
    id 'java'
    id 'io.quarkus'
}

dependencies {
    implementation enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}")

    implementation project(':cdc-api-model')
    implementation project(':cdc-core')
    implementation project(':cdc-postgres')

    implementation 'io.quarkus:quarkus-rest'
    implementation 'io.quarkus:quarkus-jdbc-postgresql'
    implementation 'io.quarkus:quarkus-smallrye-health'
    implementation 'io.quarkus:quarkus-messaging'

    testImplementation 'io.quarkus:quarkus-junit5'
}
```

### First config object

#### `ConnectorRuntimeConfig.java`

```java
package com.example.cdc.runtime.config;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "cdc")
public interface ConnectorRuntimeConfig {
    String connectorId();
    String sourceJdbcUrl();
    String sourceUsername();
    String sourcePassword();
    String publicationName();
    String slotName();
    int snapshotChunkSize();
}
```

### First services

#### `ConnectorService.java`

```java
package com.example.cdc.runtime.service;

public interface ConnectorService {
    void start();
    void stop();
}
```

#### `SnapshotService.java`

```java
package com.example.cdc.runtime.service;

import com.example.cdc.model.TableId;

public interface SnapshotService {
    void triggerTableSnapshot(TableId tableId);
}
```

### First messaging bridge

#### `WalIngressChannelBridge.java`

```java
package com.example.cdc.runtime.messaging;

import com.example.cdc.model.ChangeEnvelope;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class WalIngressChannelBridge {

    @Inject
    @Channel("wal-normalized")
    Emitter<ChangeEnvelope> emitter;

    public void emit(ChangeEnvelope event) {
        emitter.send(event);
    }
}
```

#### `PublishRequestProcessor.java`

```java
package com.example.cdc.runtime.messaging;

import com.example.cdc.core.sink.EventSink;
import com.example.cdc.model.ChangeEnvelope;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class PublishRequestProcessor {

    @Inject
    EventSink eventSink;

    @Incoming("publish-request")
    public void onEvent(ChangeEnvelope event) {
        eventSink.publish(event);
    }
}
```

### First REST resource

#### `ConnectorResource.java`

```java
package com.example.cdc.runtime.resource;

import com.example.cdc.runtime.service.ConnectorService;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

@Path("/connectors")
public class ConnectorResource {

    @Inject
    ConnectorService connectorService;

    @POST
    @Path("/start")
    public Response start() {
        connectorService.start();
        return Response.accepted().build();
    }
}
```

### Notes

Keep the runtime shell thin.

Avoid burying core logic in CDI beans.

---

## 5. `cdc-test-kit`

### Purpose

Reusable test support for integration and simulation tests.

### Suggested package structure

```text
cdc-test-kit/
  src/main/java/com/example/cdc/testkit/
    postgres/
    fixtures/
    assertions/
```

### Suggested classes

```text
com/example/cdc/testkit/postgres/
  PostgresContainerSupport.java
  TestSchemaLoader.java

com/example/cdc/testkit/fixtures/
  CustomerTableFixture.java
  AccountTableFixture.java

com/example/cdc/testkit/assertions/
  ChangeEnvelopeAssertions.java
```

### First classes

#### `PostgresContainerSupport.java`

```java
package com.example.cdc.testkit.postgres;

import org.testcontainers.containers.PostgreSQLContainer;

public final class PostgresContainerSupport {
    public static PostgreSQLContainer<?> newContainer() {
        return new PostgreSQLContainer<>("postgres:latest");
    }
}
```

### Notes

This module should grow as soon as you start writing conflict-heavy integration tests.

---

## Suggested package creation order

Do not create everything at once.

Use this order.

### Step 1

Create:

* `cdc-api-model`
* core model records and enums

### Step 2

Create:

* `cdc-core/sink`
* `EventSink`
* `LoggingEventSink`

### Step 3

Create:

* `cdc-postgres/metadata`
* metadata reader classes
* integration tests for PK discovery

### Step 4

Create:

* `cdc-postgres/replication`
* minimal replication client interfaces
* raw-to-normalized flow

### Step 5

Create:

* `cdc-runtime-quarkus/messaging`
* one internal channel bridge
* one publish processor

### Step 6

Create:

* snapshot classes
* watermark classes
* reconciliation classes

### Step 7

Create:

* REST resource classes
* checkpoint classes
* test-kit support for snapshot overlap scenarios

This order keeps the project usable at each step.

---

## First integration tests to create

These should exist very early.

### Metadata tests

* reads primary key for `customer(id)`
* reads composite primary key for a test table

### WAL tests

* insert emits one change event
* update emits one change event
* delete emits one change event

### Snapshot tests

* chunk reads first N rows by PK order
* next chunk resumes after the last key

### Reconciliation tests

* overlapping update removes matching snapshot row
* overlapping delete removes matching snapshot row
* unrelated live updates do not remove unrelated snapshot rows

---

## Suggested starter SQL objects

### Watermark table

```sql
create schema if not exists cdc_control;

create table if not exists cdc_control.watermark (
  id smallint primary key,
  value uuid not null,
  updated_at timestamptz not null default now()
);
```

### Example source table

```sql
create table if not exists public.customer (
  id bigint primary key,
  first_name text not null,
  last_name text not null,
  email text,
  updated_at timestamptz not null default now()
);
```

### Publication example

```sql
create publication cdc_pub for table public.customer, cdc_control.watermark;
```

This is enough to start proving end-to-end behavior.

---

## Suggested internal channel names

For the MVP, keep channel names simple.

* `wal-normalized`
* `reconcile-input`
* `publish-request`

You can add more later if they solve a real problem.

Do not create a large graph before you need it.

---

## Suggested application properties shape

```properties
cdc.connector-id=local-customer-cdc
cdc.source-jdbc-url=jdbc:postgresql://localhost:5432/appdb
cdc.source-username=app
cdc.source-password=app
cdc.publication-name=cdc_pub
cdc.slot-name=cdc_slot
cdc.snapshot-chunk-size=500
```

Start small and keep config easy to understand.

---

## Suggested initial TODO markers

To avoid getting lost, put TODO markers only in the places where real implementation work is still pending.

Examples:

* `TODO implement metadata query`
* `TODO decode pgoutput row messages`
* `TODO generate keyset SQL for composite PK`
* `TODO persist checkpoint after successful sink publish`

Avoid using TODO as a substitute for design.

---

## What not to add yet

Do not create these in the first cut:

* `cdc-kafka`
* `cdc-rabbitmq`
* `cdc-metadata` as a separate module
* a UI module
* generalized plugin registries
* abstract factories for every concept
* multi-DB source adapters
* scheduler modules

The first version should feel almost boring.

That is good.

---

## Recommended first coding session

If starting from zero, create things in this exact order:

1. root Gradle files
2. `cdc-api-model` records and enums
3. `cdc-core` sink interfaces and logging sink
4. `cdc-postgres` metadata classes
5. one integration test reading PK metadata from PostgreSQL
6. replication interfaces and normalizer placeholders
7. Quarkus runtime module with one REST endpoint and one internal emitter

That should leave you with a running foundation and the first real test.

---

## Final recommendation

Treat this skeleton as a starting map, not a prison.

The structure is meant to give you:

* enough separation to stay clean,
* enough simplicity to move quickly,
* enough discipline to avoid a rewrite later.

If you keep the core plain, the runtime thin, and the sink replaceable, you will have a very strong base for the MVP.
