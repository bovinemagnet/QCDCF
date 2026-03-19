package com.paulsnow.qcdcf.runtime.cli;

import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import jakarta.inject.Inject;
import picocli.CommandLine.Command;

/**
 * Displays detailed version and build information about the QCDCF installation.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Command(
        name = "about",
        description = "Display version, build, and runtime information."
)
public class AboutCommand implements Runnable {

    @Inject
    ConnectorRuntimeConfig config;

    @Override
    public void run() {
        System.out.println();
        System.out.println("  QCDCF — Quarkus CDC Framework");
        System.out.println("  ════════════════════════════════════════");
        System.out.println();
        System.out.println("  Version:       0.0.0");
        System.out.println("  Author:        Paul Snow");
        System.out.println("  Architecture:  Netflix DBLog-inspired CDC");
        System.out.println();
        System.out.println("  Runtime");
        System.out.println("  ───────");
        System.out.printf("  Java:          %s (%s)%n", System.getProperty("java.version"), System.getProperty("java.vendor"));
        System.out.printf("  Quarkus:       3.32.4%n");
        System.out.printf("  OS:            %s %s (%s)%n",
                System.getProperty("os.name"), System.getProperty("os.version"), System.getProperty("os.arch"));
        System.out.println();
        System.out.println("  Configuration");
        System.out.println("  ─────────────");
        System.out.printf("  Connector ID:  %s%n", config.connector().id());
        System.out.printf("  Slot:          %s%n", config.source().slotName());
        System.out.printf("  Publication:   %s%n", config.source().publicationName());
        System.out.printf("  Chunk size:    %d%n", config.source().chunkSize());
        System.out.println();
        System.out.println("  Modules");
        System.out.println("  ───────");
        System.out.println("  cdc-api-model          Canonical event model (records, enums)");
        System.out.println("  cdc-core               Reconciliation engine, sink abstraction, checkpoints");
        System.out.println("  cdc-postgres           WAL reader, pgoutput decoder, snapshot reader");
        System.out.println("  cdc-runtime-quarkus    REST API, HTMX dashboard, Kafka sink, CLI");
        System.out.println("  cdc-test-kit           Testcontainers support, fixtures, assertions");
        System.out.println();
        System.out.println("  Pipeline");
        System.out.println("  ────────");
        System.out.println("  PostgreSQL WAL → pgoutput decoder → event normaliser");
        System.out.println("    → watermark coordinator → reconciliation engine → event sink");
        System.out.println();
    }
}
