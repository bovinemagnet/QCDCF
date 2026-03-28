# CLAUDE.md

This file provides guidance when working with code in this repository.

## Project Overview

QCDCF (Quarkus CDC Framework) — a PostgreSQL Change Data Capture framework inspired by Netflix's DBLog. Built with Quarkus 3.32.4 and Java 25. Multi-module Gradle project implementing watermark-based reconciliation for reliable CDC.

## Build Commands

```bash
gradle build                                          # Build all modules
gradle test                                           # Run all tests
gradle :cdc-api-model:test                            # Test API model module
gradle :cdc-core:test                                 # Test core module
gradle :cdc-postgres:test                             # Test PostgreSQL module
gradle :cdc-runtime-quarkus:test                      # Test runtime module
gradle :cdc-runtime-quarkus:quarkusDev                # Start dev mode (Dev UI at localhost:8080/q/dev-ui)
gradle antora                                         # Build Antora documentation
gradle build -Dquarkus.native.enabled=true            # Native build (requires GraalVM)
```

Use `gradle` or `gradle21w`/`gradle25w` (on PATH) instead of `./gradlew`.

## Module Structure

| Module | Package | Purpose |
|--------|---------|---------|
| `cdc-api-model` | `com.paulsnow.qcdcf.model` | Records and enums — no framework dependencies |
| `cdc-core` | `com.paulsnow.qcdcf.core` | Interfaces and default implementations — plain Java, no Quarkus |
| `cdc-postgres` | `com.paulsnow.qcdcf.postgres` | PostgreSQL-specific adapters (metadata, replication, snapshot, watermark) |
| `cdc-runtime-quarkus` | `com.paulsnow.qcdcf.runtime` | Quarkus shell — REST, health, HTMX dashboard, CDI bootstrap |
| `cdc-test-kit` | `com.paulsnow.qcdcf.testkit` | Testcontainers support, fixtures, assertions |

**Dependency flow:** `cdc-runtime-quarkus` → `cdc-postgres` → `cdc-core` → `cdc-api-model`. `cdc-test-kit` depends on `cdc-api-model` and `cdc-core`.

## Key Dependencies

- **Quarkus 3.32.4** — platform BOM, Arc (CDI), REST, Qute, SmallRye Health
- **PostgreSQL driver 42.7.5** — JDBC and replication protocol
- **Testcontainers 1.20.4** — integration testing with real PostgreSQL
- **JUnit 5 + AssertJ** — testing
- **SLF4J** — logging in non-Quarkus modules
- Java source/target: **25**, compiler flag `-parameters` enabled

## Architecture Principles

- **PostgreSQL-first** — strong PostgreSQL adapter, generic abstraction deferred
- **Plain Java core** — core logic testable without CDI/HTTP
- **Pluggable sink** — `EventSink` interface with `InMemoryEventSink`, `LoggingEventSink`; Kafka later
- **Watermark reconciliation** — Netflix DBLog pattern: low watermark → chunk read → high watermark → key-collision removal
- **At-least-once delivery** — explicit guarantee for MVP
- **Metric prefix** — `qcdcf_` for all Micrometer metrics

## Testing

Each module has its own tests:
- `cdc-api-model`: Unit tests for records (equality, canonical names)
- `cdc-core`: TDD tests for reconciliation engine, chunk planner, sinks
- `cdc-postgres`: Integration tests using Testcontainers (`postgres:16-alpine` with `wal_level=logical`)
- `cdc-runtime-quarkus`: Quarkus integration tests

## Documentation

- Design docs (archived): `docs/archive/` — PRD, MVP strategy, implementation plan, architecture
- Final documentation: `src/docs/` — Antora structure with AsciiDoc pages

## Docker

Four Dockerfile variants in `cdc-runtime-quarkus/src/main/docker/`: JVM, legacy-jar, native, and native-micro.
