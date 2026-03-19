Based on the Netflix article and the DBLog paper, the core product is not “just a CDC connector.” It is a **CDC platform that can do two things at once**:

1. stream low-latency changes from the transaction log, and
2. capture full table state in **chunked snapshots** without blocking writes, using **low/high watermarks** to reconcile snapshot rows with in-flight log events. ([Netflix Tech Blog][1])

That is the part worth copying.

My opinion: if you build this in Quarkus, do **not** try to clone Netflix’s implementation literally. Build the **same product behavior** and **same guarantees**, but with a cleaner architecture for your stack: Quarkus for lifecycle/config/observability, Gradle for multi-module builds, JDBC or logical replication adapters per database, Kafka as the durable downstream transport, and Quartz only where you need clustered scheduling. Quarkus supports Gradle well, provides unified datasource configuration, Kafka messaging support, Dev Services for Kafka in dev/test, and Quartz for clustered periodic jobs. ([Quarkus][2])

Below is a PRD you can use as a real starting point.
