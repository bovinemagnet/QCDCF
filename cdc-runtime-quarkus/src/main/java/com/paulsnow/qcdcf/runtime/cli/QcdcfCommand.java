package com.paulsnow.qcdcf.runtime.cli;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.Map;

/**
 * REST resource providing CLI-style information endpoints.
 * <p>
 * The original Picocli CLI was removed because {@code quarkus-picocli} conflicts
 * with the HTTP server lifecycle. Instead, CLI operations are exposed as REST
 * endpoints and can be invoked via {@code curl} or the HTMX dashboard.
 * <p>
 * Usage:
 * <pre>
 *   curl localhost:8080/api/cli/about
 *   curl localhost:8080/api/cli/version
 *   curl localhost:8080/api/cli/check
 *   curl localhost:8080/api/cli/tables
 * </pre>
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Path("/api/cli")
@Produces(MediaType.APPLICATION_JSON)
public class QcdcfCommand {

    @GET
    @Path("/version")
    public Map<String, String> version() {
        return Map.of(
                "name", "QCDCF",
                "fullName", "Quarkus CDC Framework",
                "version", "0.0.0",
                "author", "Paul Snow",
                "description", "PostgreSQL Change Data Capture inspired by Netflix DBLog",
                "java", System.getProperty("java.version"),
                "quarkus", "3.32.4"
        );
    }
}
