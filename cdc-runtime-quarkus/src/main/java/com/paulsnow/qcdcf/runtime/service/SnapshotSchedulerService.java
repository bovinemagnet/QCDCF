package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.postgres.metadata.PostgresTableMetadataReader;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;

/**
 * Triggers periodic snapshots for all published tables on a configurable cron schedule.
 * <p>
 * Skips execution if a snapshot is already running or if scheduling is disabled.
 * Discovers tables from the PostgreSQL publication and triggers each sequentially.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class SnapshotSchedulerService {

    private static final Logger LOG = Logger.getLogger(SnapshotSchedulerService.class);

    @Inject
    ConnectorRuntimeConfig config;

    @Inject
    ConnectorService connectorService;

    @Inject
    DataSource dataSource;

    @Scheduled(cron = "{qcdcf.snapshot.schedule.cron}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void scheduledSnapshot() {
        if (!config.snapshot().schedule().enabled()) {
            return;
        }

        if (connectorService.isSnapshotRunning()) {
            LOG.warn("Scheduled snapshot skipped — a snapshot is already running");
            return;
        }

        LOG.info("Scheduled snapshot starting — discovering published tables");

        try (Connection conn = dataSource.getConnection()) {
            var metadataReader = new PostgresTableMetadataReader();
            List<TableId> tables = metadataReader.discoverPublicationTables(
                    conn, config.source().publicationName());

            if (tables.isEmpty()) {
                LOG.warnf("Scheduled snapshot — no tables found in publication '%s'",
                        config.source().publicationName());
                return;
            }

            LOG.infof("Scheduled snapshot — %d tables to snapshot", tables.size());

            for (TableId tableId : tables) {
                LOG.infof("Scheduled snapshot — triggering for %s", tableId);
                connectorService.triggerSnapshot(tableId.canonicalName());

                // Wait for this snapshot to complete before starting the next
                while (connectorService.isSnapshotRunning()) {
                    Thread.sleep(1000);
                }
            }

            LOG.info("Scheduled snapshot complete — all tables processed");

        } catch (Exception e) {
            LOG.errorf(e, "Scheduled snapshot failed: %s", e.getMessage());
        }
    }
}
