package com.paulsnow.qcdcf.runtime.cli;

import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST endpoint providing detailed version and runtime information.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Path("/api/cli")
@Produces(MediaType.APPLICATION_JSON)
public class AboutCommand {

    @Inject
    ConnectorRuntimeConfig config;

    @GET
    @Path("/about")
    public Map<String, Object> about() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", "QCDCF");
        info.put("fullName", "Quarkus CDC Framework");
        info.put("version", "0.0.0");
        info.put("author", "Paul Snow");
        info.put("architecture", "Netflix DBLog-inspired CDC");

        Map<String, String> runtime = new LinkedHashMap<>();
        runtime.put("java", System.getProperty("java.version") + " (" + System.getProperty("java.vendor") + ")");
        runtime.put("quarkus", "3.32.4");
        runtime.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " (" + System.getProperty("os.arch") + ")");
        info.put("runtime", runtime);

        Map<String, Object> configuration = new LinkedHashMap<>();
        configuration.put("connectorId", config.connector().id());
        configuration.put("slotName", config.source().slotName());
        configuration.put("publicationName", config.source().publicationName());
        configuration.put("chunkSize", config.source().chunkSize());
        configuration.put("sinkType", config.sink().type());
        info.put("configuration", configuration);

        info.put("modules", Map.of(
                "cdc-api-model", "Canonical event model (records, enums)",
                "cdc-core", "Reconciliation engine, sink abstraction, checkpoints",
                "cdc-postgres", "WAL reader, pgoutput decoder, snapshot reader",
                "cdc-runtime-quarkus", "REST API, HTMX dashboard, Kafka sink, CLI",
                "cdc-test-kit", "Testcontainers support, fixtures, assertions"
        ));

        return info;
    }
}
