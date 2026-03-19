package com.paulsnow.qcdcf.runtime.cli;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import picocli.CommandLine.Command;

/**
 * Top-level CLI command for the QCDCF framework.
 * <p>
 * Subcommands provide operational control over the CDC connector:
 * starting the service, triggering snapshots, checking prerequisites,
 * listing monitored tables, and querying status.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@TopCommand
@Command(
        name = "qcdcf",
        mixinStandardHelpOptions = true,
        version = {
                "QCDCF (Quarkus CDC Framework) v0.0.0",
                "PostgreSQL Change Data Capture inspired by Netflix DBLog",
                "",
                "Author:  Paul Snow",
                "Runtime: Quarkus 3.32.4, Java ${java.version}",
                "Licence: Proprietary"
        },
        header = {
                "",
                "  ┌─────────────────────────────────────────┐",
                "  │  QCDCF — Quarkus CDC Framework          │",
                "  │  PostgreSQL Change Data Capture          │",
                "  │  Inspired by Netflix DBLog               │",
                "  └─────────────────────────────────────────┘",
                ""
        },
        description = {
                "",
                "QCDCF captures row-level changes from PostgreSQL using logical replication",
                "and delivers them to downstream systems via a pluggable sink architecture.",
                "",
                "Key capabilities:",
                "  • Low-latency WAL streaming via pgoutput protocol",
                "  • Chunked snapshots without table locks (keyset pagination)",
                "  • Watermark-based reconciliation (Netflix DBLog pattern)",
                "  • Pluggable sinks: logging, Kafka, or custom",
                "  • HTMX operational dashboard at /dashboard",
                "  • REST admin API at /api/connector/*",
                ""
        },
        footer = {
                "",
                "Examples:",
                "  qcdcf start                     Start the connector service",
                "  qcdcf start -p 9090             Start on a custom port",
                "  qcdcf check                     Validate PostgreSQL prerequisites",
                "  qcdcf tables                    List monitored tables",
                "  qcdcf snapshot public.customer   Trigger a table snapshot",
                "  qcdcf status                    Show connector status",
                "  qcdcf status -r http://host:8080  Query a remote instance",
                "",
                "Documentation: build/docs/site/index.html (run 'gradle antora' first)",
                ""
        },
        subcommands = {
                StartCommand.class,
                SnapshotCommand.class,
                StatusCommand.class,
                TablesCommand.class,
                CheckCommand.class,
                AboutCommand.class
        }
)
public class QcdcfCommand {
}
