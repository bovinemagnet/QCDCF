package com.paulsnow.qcdcf.runtime.cli;

import com.paulsnow.qcdcf.runtime.service.ConnectorService;
import jakarta.inject.Inject;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.Map;

/**
 * Triggers a one-off table snapshot via the CLI.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Command(
        name = "snapshot",
        description = "Trigger a table snapshot (e.g. qcdcf snapshot public.customer)."
)
public class SnapshotCommand implements Runnable {

    @Parameters(index = "0", description = "Fully-qualified table name (e.g. public.customer)")
    String tableName;

    @Inject
    ConnectorService connectorService;

    @Override
    public void run() {
        System.out.println("Triggering snapshot for table: " + tableName);
        Map<String, Object> result = connectorService.triggerSnapshot(tableName);
        System.out.println("Snapshot status: " + result.get("snapshotStatus"));
        System.out.println("Message: " + result.get("message"));

        // Wait for the background snapshot to complete
        System.out.print("Waiting for snapshot to complete");
        String status = connectorService.lastSnapshotStatus();
        int maxWait = 300; // 5 minutes max
        int waited = 0;
        while (status.equals("RUNNING") && waited < maxWait) {
            try {
                Thread.sleep(1000);
                waited++;
                if (waited % 5 == 0) System.out.print(".");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            status = connectorService.lastSnapshotStatus();
        }

        System.out.println();
        System.out.println("Final status: " + status);
    }
}
