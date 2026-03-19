# Performance and Volume Testing Strategy for the PostgreSQL CDC Framework

## Purpose

This document defines how performance testing, scale testing, and synthetic load testing will be approached for the PostgreSQL CDC framework.

It is a companion to the PRD, technical architecture specification, MVP strategy, implementation plan, and project skeleton.

Its purpose is to ensure that performance and scale are treated as first-class engineering concerns from the beginning, without forcing premature optimization into the MVP.

This document focuses on four outcomes:

1. proving that the framework remains correct under load,
2. identifying throughput and latency limits early,
3. understanding database and runtime bottlenecks before production rollout,
4. building a repeatable synthetic test methodology that can be rerun as the system evolves.

---

## Core principle

For this system, performance testing is not only about speed.

It is also about:

* correctness under concurrency,
* predictable backlog behavior,
* restart behavior under pressure,
* safe interaction with the source database,
* realistic operational limits.

A fast result with incorrect ordering, missed rows, excessive database impact, or unstable restart behavior is a failed result.

---

## Why synthetic testing is essential

Real production-scale CDC behavior is hard to infer from low-volume integration tests.

This framework must eventually handle combinations of:

* high insert rates,
* hot-key update storms,
* large table snapshots,
* overlapping live writes during snapshot windows,
* bursty downstream sink performance,
* long-running WAL streams,
* recovery after failure while lag exists.

These conditions are difficult to reproduce reliably with ordinary functional tests.

Synthetic load is therefore required to:

* generate repeatable high-volume conditions,
* isolate bottlenecks,
* compare changes between builds,
* expose algorithmic weaknesses before production.

---

## Testing objectives

Performance and volume testing should answer these questions.

### 1. Source ingestion capacity

How many row changes per second can the framework read from PostgreSQL before lag grows unacceptably?

### 2. Snapshot throughput

How quickly can chunked snapshots progress for large tables without causing unacceptable source database impact?

### 3. Reconciliation cost

How much memory, CPU, and latency does watermark reconciliation consume as overlap and collision rates increase?

### 4. Sink behavior under pressure

What happens when the sink is slower than the incoming WAL stream?

### 5. Restart and recovery under backlog

Can the system resume safely and predictably when there is already substantial lag or an incomplete snapshot job?

### 6. Operational scaling envelope

What are the practical limits for:

* rows per second,
* tables per connector,
* chunk sizes,
* open watermark windows,
* memory footprint,
* acceptable database overhead?

---

## Scope of performance testing

This testing strategy covers five distinct categories.

## 1. Micro-benchmarking

Used for small internal components where algorithmic efficiency matters.

Examples:

* row-key comparison,
* collision detection,
* event envelope serialization,
* in-memory reconciliation structures,
* keyset chunk boundary processing.

This is useful for hotspots, but it is not enough by itself.

## 2. Component performance testing

Used for testing one subsystem in isolation.

Examples:

* WAL reader throughput,
* snapshot reader throughput,
* reconciliation engine performance with synthetic streams,
* sink throughput.

## 3. End-to-end load testing

Used for realistic system behavior.

Examples:

* source writes plus WAL ingestion plus reconciliation plus sink publication,
* snapshotting while live writes continue,
* lag growth and drain behavior,
* checkpoint advancement under sustained load.

## 4. Endurance testing

Used to expose slow leaks and drift.

Examples:

* six-hour or twelve-hour WAL stream runs,
* long snapshots on large tables,
* repeated snapshot triggering,
* memory stability over time.

## 5. Failure-under-load testing

Used to validate correctness when the system is already stressed.

Examples:

* restart during active backlog,
* sink slowdown while snapshot windows are open,
* source reconnection while lag exists,
* duplicate replay from earlier WAL positions under pressure.

---

## Performance success criteria

Performance results should always be evaluated against explicit success criteria.

These criteria will evolve, but should begin with the following dimensions.

### Throughput

* WAL events processed per second
* snapshot rows processed per second
* end-to-end events published per second

### Latency

* p50, p95, p99 source-to-sink latency
* p50, p95, p99 snapshot chunk completion time
* low-to-high watermark window duration

### Resource efficiency

* CPU usage by stage
* heap and off-heap memory usage
* GC frequency and pause time
* network throughput
* disk usage where applicable

### Database impact

* source PostgreSQL CPU load
* WAL generation volume
* read query latency impact on source tables
* autovacuum interaction if relevant
* replication lag characteristics

### Correctness under load

* missed row count
* duplicate event count
* stale snapshot overwrite count
* ordering violation count
* mismatch count between source truth and emitted result

---

## Testing philosophy

The framework should be tested in stages.

### Stage 1: correct before fast

Do not trust a throughput result if correctness is not verified.

### Stage 2: isolate bottlenecks before tuning

Do not optimize all layers at once.

### Stage 3: test realistic data shapes

Large scale does not only mean row count.

It also means:

* row width,
* skewed key distribution,
* update hot spots,
* mixed operation ratios,
* multi-table contention.

### Stage 4: test under degraded sink conditions

The most misleading benchmarks are the ones where the sink is unrealistically fast.

### Stage 5: test with recovery and replay

A CDC system is not finished when it is fast on a clean startup path.

---

## Workload model

Synthetic load must represent more than one behavior pattern.

The framework should be tested against several named workload models.

## Workload A: append-heavy stream

Characteristics:

* mostly inserts
* low update frequency
* limited deletes
* moderate row width

Use case:

* event-like tables
* audit-like growth

Purpose:

* validates maximum ingestion throughput with simpler overlap patterns

## Workload B: mixed OLTP pattern

Characteristics:

* inserts, updates, and deletes
* moderate concurrency
* typical primary-key distribution
* moderate row width

Use case:

* common application tables

Purpose:

* baseline realistic workload

## Workload C: hot-key contention

Characteristics:

* repeated updates to a small set of keys
* high overlap probability inside watermark windows
* moderate-to-high event rate

Purpose:

* stresses reconciliation correctness and memory behavior

## Workload D: wide-row workload

Characteristics:

* large payload columns
* higher serialization cost
* slower chunk reads
* higher memory pressure

Purpose:

* exposes payload handling bottlenecks

## Workload E: large snapshot with background writes

Characteristics:

* very large table
* chunked snapshot over a long duration
* ongoing writes to keys both inside and outside current chunk ranges

Purpose:

* validates the core DBLog-inspired operating mode

## Workload F: sink-constrained workload

Characteristics:

* source writes faster than sink publication
* intentional sink delay or throttling

Purpose:

* validates backpressure, lag growth, and checkpoint behavior

---

## Synthetic data model

To avoid misleading results, synthetic data should be generated with realistic variation.

### Suggested source tables

At minimum, create synthetic versions of:

* small narrow-row table
* medium mixed-row table
* very large table with stable primary key
* wide-row table with many text columns
* hot-key table designed for repeated updates

### Example table profiles

#### Profile 1: narrow customer-like table

* bigint primary key
* 5 to 8 simple columns
* moderate update frequency

#### Profile 2: order-like table

* bigint primary key
* status updates
* timestamp updates
* medium row width

#### Profile 3: document-like table

* bigint primary key
* wide text or JSON payload
* large serialized event size

#### Profile 4: inventory-like hot table

* small key range
* frequent updates to same rows
* occasional deletes and inserts

---

## Data generation strategy

Synthetic load generation should support these controls.

### Controls

* target rows per second
* operation mix percentages

  * insert
  * update
  * delete
* number of active writers
* key distribution strategy

  * uniform
  * skewed
  * hot subset
* row width profile
* transaction batch size
* snapshot trigger timing
* sink throttling profile

### Recommended generator design

Create a standalone load generator that can:

* connect to PostgreSQL directly,
* generate configurable transactional load,
* record expected source mutations for later correctness comparison,
* coordinate with the CDC runtime for snapshot-trigger scenarios.

This generator should not be tightly coupled to the main runtime.

It should be a separate tool or submodule so it can be run independently and tuned freely.

---

## Test environments

Performance and scale tests should be run in distinct environments.

## 1. Developer environment

Purpose:

* smoke-level performance checks
* correctness under small synthetic load

Characteristics:

* local machine or laptop
* Docker/Testcontainers acceptable
* small datasets
* short-duration tests

## 2. CI performance sanity environment

Purpose:

* catch major regressions automatically

Characteristics:

* limited volume
* shorter test duration
* fixed scenario set
* stable metrics and thresholds

## 3. dedicated performance environment

Purpose:

* realistic volume and tuning work

Characteristics:

* dedicated PostgreSQL instance
* dedicated runtime host or cluster
* sufficient disk, CPU, and memory
* isolated network path where possible

## 4. pre-production scale environment

Purpose:

* realistic final validation before production adoption

Characteristics:

* larger datasets
* longer test durations
* production-like config
* operational dashboards enabled

---

## Test harness architecture

A repeatable performance harness should include the following components.

### 1. source data loader

Seeds baseline rows into large test tables.

### 2. synthetic load generator

Continuously applies inserts, updates, and deletes according to a scenario profile.

### 3. CDC runtime under test

Runs the actual framework build being evaluated.

### 4. sink observer

Measures publication throughput, latency, ordering, and duplicates.

### 5. correctness verifier

Compares final emitted state or event-derived state against source truth.

### 6. metrics collector

Captures runtime, JVM, PostgreSQL, and sink metrics.

### 7. scenario controller

Triggers snapshots, sink slowdowns, restarts, and fault injection at precise times.

---

## Measurement points

The system should expose measurement points at each major boundary.

### Source-side measurements

* write throughput generated by the load tool
* PostgreSQL CPU and memory
* WAL generation rate
* replication lag at source
* query latency for snapshot selects

### Runtime measurements

* raw WAL read throughput
* normalized event throughput
* internal channel queue depth or pressure indicators
* reconciliation buffer size
* snapshot chunk throughput
* time spent in chunk reads
* time spent in reconciliation
* checkpoint commit rate
* GC and heap usage

### Sink-side measurements

* sink publish throughput
* publish acknowledgment latency
* sink backpressure or queue depth
* failed publish count

### End-to-end measurements

* source commit to sink publish latency
* snapshot trigger to chunk completion time
* lag growth during stress
* lag drain time after load spike

---

## Metrics that must exist

The runtime should expose at least these metrics before serious scale testing begins.

### WAL and ingestion

* `cdc_wal_events_received_total`
* `cdc_wal_events_processed_total`
* `cdc_wal_bytes_received_total`
* `cdc_wal_lag_bytes`
* `cdc_source_commit_to_ingest_ms`

### Snapshot

* `cdc_snapshot_chunks_started_total`
* `cdc_snapshot_chunks_completed_total`
* `cdc_snapshot_rows_read_total`
* `cdc_snapshot_chunk_duration_ms`
* `cdc_snapshot_active_windows`

### Reconciliation

* `cdc_reconciliation_windows_total`
* `cdc_reconciliation_collisions_total`
* `cdc_reconciliation_window_duration_ms`
* `cdc_reconciliation_buffer_entries`

### Sink

* `cdc_sink_publish_total`
* `cdc_sink_publish_failures_total`
* `cdc_sink_publish_latency_ms`
* `cdc_sink_backlog_size`

### Correctness

* `cdc_duplicate_events_total`
* `cdc_ordering_violations_total`
* `cdc_mismatch_rows_total`
* `cdc_stale_snapshot_conflicts_total`

These names can change, but the concepts should not.

---

## Test scenario catalog

The following scenarios should be built and automated over time.

## Scenario 1: baseline WAL throughput

Objective:

* find maximum stable event ingestion rate without snapshot activity

Method:

* no snapshot jobs
* source generates inserts and updates only
* sink uses fast in-memory or logging sink first, then durable sink later

Measure:

* max sustainable events/sec
* ingest latency
* heap growth
* WAL lag behavior

## Scenario 2: baseline snapshot throughput

Objective:

* measure raw chunk-scan performance on large tables

Method:

* seed a large table
* trigger snapshot without concurrent writes
* vary chunk size

Measure:

* rows/sec
* chunk duration
* database CPU impact
* memory behavior

## Scenario 3: snapshot with ongoing writes

Objective:

* validate the core online snapshot behavior under realistic overlap

Method:

* run steady updates during snapshot
* vary update intensity
* vary chunk size

Measure:

* snapshot completion time
* collision count
* stale row prevention correctness
* source DB impact

## Scenario 4: hot-key conflict storm

Objective:

* stress collision handling and event ordering

Method:

* repeatedly update a small subset of keys while snapshot windows are open

Measure:

* reconciliation cost
* memory growth
* ordering correctness
* duplicate behavior

## Scenario 5: sink throttling

Objective:

* understand system behavior when the sink is the bottleneck

Method:

* add controlled sink delays
* keep source write rate high

Measure:

* lag growth rate
* buffer growth
* checkpoint lag
* stability over time

## Scenario 6: recovery under backlog

Objective:

* verify restart safety while lag exists

Method:

* generate backlog
* restart the service during lag
* continue writes after restart

Measure:

* recovery time
* duplicate rate
* missed event rate
* final correctness

## Scenario 7: endurance run

Objective:

* detect leaks and slow degradation

Method:

* run mixed load for many hours
* periodically trigger snapshots

Measure:

* memory trend
* GC trend
* throughput drift
* database drift
* checkpoint consistency

---

## Correctness verification under load

This is critical.

Every serious load scenario should include a correctness check.

### Verification approaches

## 1. final-state verification

For snapshot-heavy scenarios, derive final state from emitted events and compare it to source truth at the end of the test.

## 2. event-stream invariants

Validate invariants such as:

* event order per key is preserved,
* no stale snapshot row appears after a newer live change for the same key,
* deletes are not undone by older snapshot rows,
* watermark windows do not leak rows incorrectly.

## 3. sampled truth validation

For very large runs, compare a statistically meaningful sample of keys between source and emitted result.

## 4. full truth validation for bounded runs

For smaller bounded tests, compare all rows.

The testing framework should produce a correctness report, not just throughput charts.

---

## Fault injection during performance testing

The system must be tested under failure while load is active.

### Faults to inject

* sink delay
* sink temporary failure
* PostgreSQL connection interruption
* service restart
* snapshot cancellation
* checkpoint persistence delay
* reduced CPU or memory availability

### Why this matters

A CDC system at scale is often constrained not by the happy path, but by degraded conditions combined with backlog.

---

## Dataset scale levels

Define named scale levels so results are comparable.

### Scale Level S

* up to 1 million rows
* low concurrency
* correctness-focused

### Scale Level M

* 10 to 50 million rows
* moderate concurrency
* first realistic throughput measurements

### Scale Level L

* 100 million plus rows
* sustained concurrent writes
* long snapshot windows
* significant lag and drain behavior

### Scale Level XL

* beyond initial MVP target
* only after tuning and operational hardening

These can be adjusted, but the naming convention helps compare runs cleanly.

---

## Parameter sweeps

Performance testing should include controlled sweeps across key parameters.

### Parameters to sweep

* chunk size
* number of source writers
* transaction batch size
* sink delay
* row width profile
* hot-key percentage
* concurrent tables enabled
* heap size
* internal channel buffer settings if applicable

### Goal

Do not search for one “best” number casually.

Instead, characterize performance curves so safe operating ranges become visible.

---

## Reporting format

Every performance run should produce a structured report.

### Report contents

* scenario name
* code version or git commit
* configuration used
* dataset profile
* runtime environment details
* duration
* throughput results
* latency percentiles
* resource usage summary
* database impact summary
* correctness summary
* anomalies observed
* recommended next tuning actions

Results should be stored so trends across versions can be reviewed.

---

## Tooling recommendations

The exact tools can change, but the strategy should include:

### For source load generation

* a custom Java load generator or small dedicated tool
* optionally SQL-based seeding scripts

### For benchmarking internal components

* JMH for micro-benchmarks

### For runtime metrics

* Micrometer and Prometheus scraping
* JVM metrics
* PostgreSQL metrics exporter if available

### For visualization

* Grafana dashboards
* structured test result output in JSON or markdown

### For correctness comparison

* a verification job that reconstructs final state from emitted events and compares to source tables

---

## What not to do

To avoid misleading conclusions, do not do the following.

### 1. Do not test only with inserts

That hides the hard part of reconciliation.

### 2. Do not test only with tiny rows

That underestimates serialization and memory cost.

### 3. Do not ignore the source database impact

A fast CDC runtime that harms PostgreSQL is not acceptable.

### 4. Do not treat sink-free tests as production-scale proof

They are useful, but incomplete.

### 5. Do not claim scale readiness without restart testing

Recovery behavior matters at least as much as raw throughput.

### 6. Do not optimize before instrumentation exists

Without metrics, tuning is guesswork.

---

## Performance maturity roadmap

The testing approach should evolve with the product.

## MVP phase

Focus on:

* correctness under moderate synthetic load
* one-table and few-table scenarios
* baseline throughput
* baseline snapshot cost
* initial sink throttling tests

## hardening phase

Focus on:

* large dataset testing
* endurance runs
* restart under backlog
* chunk-size tuning
* memory tuning
* first durable sink measurements

## scale phase

Focus on:

* 100M+ row tables
* long-running mixed workloads
* source DB impact limits
* operational envelope definition
* capacity planning data

---

## Exit criteria for scale readiness

The system should not be considered ready for larger-scale rollout until all of the following are true.

* target workload scenarios have been executed repeatedly
* throughput and latency are measured and documented
* correctness is verified under load
* restart and replay behavior has been tested under backlog
* chunk size and sink behavior have been characterized
* source database overhead stays within agreed limits
* no major memory or stability regressions appear in endurance runs

---

## Final recommendation

Treat performance and volume testing as a product capability, not a late-stage exercise.

Build the harness early, keep the scenarios repeatable, and always combine throughput measurements with correctness verification.

For this CDC framework, the right question is not merely:

"How fast is it?"

It is:

"How fast is it while remaining correct, restart-safe, and operationally safe for PostgreSQL under realistic load?"

That is the standard the framework should be built to meet.
