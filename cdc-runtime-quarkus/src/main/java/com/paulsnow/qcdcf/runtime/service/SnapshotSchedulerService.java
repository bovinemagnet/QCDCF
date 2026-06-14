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
 * Discovers tables from the PostgreSQL publication and snapshots each sequentially,
 * blocking until one completes before starting the next. A failed table is logged
 * and the run continues with the remaining tables.
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

        String publicationName = config.source().publicationName();
        List<TableId> tables;
        try (Connection conn = dataSource.getConnection()) {
            tables = new PostgresTableMetadataReader().discoverPublicationTables(conn, publicationName);
        } catch (Exception e) {
            LOG.errorf(e, "Scheduled snapshot failed during table discovery: %s", e.getMessage());
            return;
        }

        if (tables.isEmpty()) {
            LOG.warnf("Scheduled snapshot — no tables found in publication '%s'", publicationName);
            return;
        }

        LOG.infof("Scheduled snapshot — %d tables to snapshot", tables.size());

        int failures = 0;
        for (TableId tableId : tables) {
            LOG.infof("Scheduled snapshot — running for %s", tableId);
            if (!connectorService.runSnapshotBlocking(tableId)) {
                failures++;
            }
        }

        if (failures == 0) {
            LOG.info("Scheduled snapshot complete — all tables processed");
        } else {
            LOG.warnf("Scheduled snapshot complete — %d of %d tables failed", failures, tables.size());
        }
    }
}
