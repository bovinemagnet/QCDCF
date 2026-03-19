package com.paulsnow.qcdcf.runtime.cli;

import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import com.paulsnow.qcdcf.runtime.service.ConnectorService;
import jakarta.inject.Inject;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;

/**
 * Queries the status of a running QCDCF connector instance.
 * <p>
 * By default queries the local instance via injected service. With {@code --remote},
 * queries a running instance via its REST API.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Command(
        name = "status",
        description = "Show connector status and progress."
)
public class StatusCommand implements Runnable {

    @Option(names = {"-r", "--remote"}, description = "Query a remote instance via REST API")
    String remoteUrl;

    @Inject
    ConnectorService connectorService;

    @Override
    public void run() {
        if (remoteUrl != null && !remoteUrl.isBlank()) {
            queryRemote();
        } else {
            queryLocal();
        }
    }

    private void queryLocal() {
        Map<String, Object> progress = connectorService.getProgress();
        System.out.println("QCDCF Connector Status");
        System.out.println("======================");
        progress.forEach((key, value) ->
                System.out.printf("  %-25s %s%n", key + ":", value));
    }

    private void queryRemote() {
        String url = remoteUrl.endsWith("/") ? remoteUrl : remoteUrl + "/";
        url += "api/connector/progress";
        try {
            HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            int status = conn.getResponseCode();
            if (status == 200) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    System.out.println("QCDCF Remote Status (" + remoteUrl + ")");
                    System.out.println("=".repeat(40));
                    reader.lines().forEach(System.out::println);
                }
            } else {
                System.err.println("Failed to query remote instance: HTTP " + status);
            }
        } catch (Exception e) {
            System.err.println("Failed to connect to " + url + ": " + e.getMessage());
        }
    }
}
