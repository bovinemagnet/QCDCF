package com.paulsnow.qcdcf.runtime.cli;

import io.quarkus.runtime.Quarkus;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Starts the QCDCF connector as a long-running service.
 * <p>
 * This is the default command. It launches the Quarkus runtime with
 * the REST API, HTMX dashboard, health endpoints, and WAL capture pipeline.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Command(
        name = "start",
        description = "Start the CDC connector service (REST API + WAL capture pipeline)."
)
public class StartCommand implements Runnable {

    @Option(names = {"-p", "--port"}, description = "HTTP port (default: 8080)", defaultValue = "8080")
    int port;

    @Override
    public void run() {
        System.out.println("Starting QCDCF connector on port " + port + "...");
        System.setProperty("quarkus.http.port", String.valueOf(port));
        Quarkus.waitForExit();
    }
}
