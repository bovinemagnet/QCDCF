# QCDCF — Quarkus CDC Framework

A PostgreSQL Change Data Capture framework inspired by Netflix's DBLog, built with Quarkus, Picocli, and Java 25.

QCDCF combines transaction-log event streaming with chunked table reads, using a watermark-based coordination model to merge both streams safely — without pausing log processing or requiring locks.

## Status

Early scaffold phase. The project currently provides a Picocli CLI starter. Full CDC implementation is planned — see the [design documents](#documentation) for details.

## Features (Planned)

- Stream committed row changes from PostgreSQL with low latency
- Chunked table reads for full-state capture without downtime
- Watermark-based reconciliation of snapshot rows and in-flight log changes
- Full, per-table, and targeted primary-key snapshot support
- Pluggable sink abstraction (Kafka, in-process messaging, and more)
- REST admin API for operations and monitoring

## Prerequisites

- Java 25
- Gradle (via `gradle21w` wrapper on PATH)
- PostgreSQL (for CDC features, when implemented)
- GraalVM (optional, for native builds)

## Build and Run

```bash
# Start in dev mode (Dev UI at http://localhost:8080/q/dev-ui)
gradle21w quarkusDev

# Build the project
gradle21w build

# Run tests
gradle21w test

# Build a native executable (requires GraalVM)
gradle21w build -Dquarkus.native.enabled=true
```

## Documentation

Design documents live in `docs/`:

| Document | Description |
|----------|-------------|
| `docs/prd/qcdcf-prd.md` | Full product requirements document |
| `docs/mvp.md` | MVP strategy — watermark-based reconciliation focus |
| `docs/implementation-plan.md` | Implementation planning |
| `docs/architecture/overview.md` | Architecture overview |

Final documentation uses the Antora structure in `src/docs/` and can be built with:

```bash
gradle21w antora
```

## Technology Stack

- **Quarkus 3.32.4** — platform BOM, Arc (CDI), Picocli extension
- **Java 25** — source and target compatibility
- **JUnit 5** — via Quarkus test support
- **Gradle** — build tool
- **Picocli** — CLI framework

## Licence

See the project licence file for details.
