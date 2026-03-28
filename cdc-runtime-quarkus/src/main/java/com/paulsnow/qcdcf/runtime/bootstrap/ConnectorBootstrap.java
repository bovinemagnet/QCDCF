package com.paulsnow.qcdcf.runtime.bootstrap;

import com.paulsnow.qcdcf.core.connector.ConnectorStatus;
import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.postgres.replication.PgOutputEventNormaliser;
import com.paulsnow.qcdcf.postgres.replication.PgOutputMessageDecoder;
import com.paulsnow.qcdcf.postgres.replication.PostgresLogStreamReader;
import com.paulsnow.qcdcf.postgres.replication.PostgresLogicalReplicationClient;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import com.paulsnow.qcdcf.runtime.service.MetricsService;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.context.ManagedExecutor;

/**
 * Bootstraps the CDC connector on application startup.
 * <p>
 * Creates the WAL capture pipeline components (replication client, decoder, normaliser)
 * and wires them through a metrics-aware {@link EventSink} decorator. The blocking
 * replication reader runs on a dedicated daemon thread.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class ConnectorBootstrap {

    private static final Logger LOG = Logger.getLogger(ConnectorBootstrap.class);

    private final ConnectorRuntimeConfig config;

    @Inject
    EventSink eventSink;

    @Inject
    MetricsService metricsService;

    @ConfigProperty(name = "quarkus.datasource.jdbc.url")
    String jdbcUrl;

    @ConfigProperty(name = "quarkus.datasource.username")
    String username;

    @ConfigProperty(name = "quarkus.datasource.password")
    String password;

    @Inject
    ManagedExecutor executor;

    private volatile ConnectorStatus status = ConnectorStatus.STOPPED;
    private PostgresLogStreamReader reader;

    public ConnectorBootstrap(ConnectorRuntimeConfig config) {
        this.config = config;
    }

    void onStart(@Observes StartupEvent event) {
        LOG.infof("QCDCF connector '%s' starting (auto-start=%s)", config.connector().id(), config.connector().autoStart());

        if (!config.connector().autoStart()) {
            status = ConnectorStatus.STOPPED;
            LOG.infof("QCDCF connector '%s' auto-start is disabled — use REST API or dashboard to start", config.connector().id());
            return;
        }

        startWalReader();
    }

    /**
     * Builds and starts the WAL reader pipeline on a background thread.
     * On failure, sets status to FAILED with a descriptive error message
     * but does NOT crash the application — the REST API and dashboard remain available.
     */
    public void startWalReader() {
        status = ConnectorStatus.STARTING;

        PostgresLogicalReplicationClient client = new PostgresLogicalReplicationClient(
                jdbcUrl, username, password,
                config.source().slotName(),
                config.source().publicationName()
        );

        PgOutputMessageDecoder decoder = new PgOutputMessageDecoder();
        PgOutputEventNormaliser normaliser = new PgOutputEventNormaliser(config.connector().id());

        EventSink metricsSink = new EventSink() {
            @Override
            public PublishResult publish(ChangeEnvelope event) {
                PublishResult result = eventSink.publish(event);
                if (result.isSuccess()) {
                    metricsService.recordEvent(event);
                } else {
                    var failure = (PublishResult.Failure) result;
                    metricsService.recordError(String.format("Sink publication failed for %s: %s",
                            event.tableId(), failure.reason()));
                }
                return result;
            }

            @Override
            public PublishResult publishBatch(List<ChangeEnvelope> events) {
                PublishResult result = eventSink.publishBatch(events);
                if (result.isSuccess()) {
                    events.forEach(metricsService::recordEvent);
                }
                return result;
            }
        };

        reader = new PostgresLogStreamReader(client, decoder, normaliser, metricsSink);

        executor.submit(() -> {
            try {
                status = ConnectorStatus.RUNNING;
                LOG.infof("QCDCF connector '%s' is running", config.connector().id());
                reader.start(0);
            } catch (Exception e) {
                String hint = diagnoseFailure(e);
                LOG.errorf("WAL reader failed for connector '%s': %s%s",
                        config.connector().id(), e.getMessage(), hint);
                lastError = e.getMessage() + hint;
                status = ConnectorStatus.FAILED;
            }
        });
    }

    private String lastError;

    /** Returns the last error message, or null if no error. */
    public String lastError() {
        return lastError;
    }

    /** Provides actionable hints based on common failure causes. */
    private static String diagnoseFailure(Exception e) {
        String msg = e.getMessage() != null ? e.getMessage() : "";
        if (e.getCause() != null && e.getCause().getMessage() != null) {
            msg += " " + e.getCause().getMessage();
        }

        if (msg.contains("permission denied to start WAL sender") || msg.contains("REPLICATION attribute")) {
            return "\n  → Fix: ALTER ROLE <username> WITH REPLICATION;\n"
                 + "  → See: /api/cli/check or the operations documentation";
        }
        if (msg.contains("replication slot") && msg.contains("does not exist")) {
            return "\n  → Fix: SELECT pg_create_logical_replication_slot('<slot>', 'pgoutput');\n"
                 + "  → Or the slot will be created automatically if the user has CREATE privileges";
        }
        if (msg.contains("publication") && msg.contains("does not exist")) {
            return "\n  → Fix: CREATE PUBLICATION <name> FOR TABLE <table1>, <table2>;\n";
        }
        if (msg.contains("Connection refused") || msg.contains("connect")) {
            return "\n  → Fix: Check quarkus.datasource.jdbc.url and ensure PostgreSQL is running";
        }
        return "";
    }

    void onStop(@Observes ShutdownEvent event) {
        LOG.infof("QCDCF connector '%s' shutting down", config.connector().id());
        if (reader != null) {
            reader.stop();
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.warnf("Executor did not terminate within 10 seconds for connector '%s'", config.connector().id());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        status = ConnectorStatus.STOPPED;
        LOG.infof("QCDCF connector '%s' stopped", config.connector().id());
    }

    public ConnectorStatus status() {
        return status;
    }

    public String connectorId() {
        return config.connector().id();
    }

    /**
     * Request the connector to stop (pause).
     * Stops the WAL reader and sets the status to STOPPED.
     */
    public void requestStop() {
        LOG.infof("Stop requested for connector '%s'", config.connector().id());
        if (reader != null) {
            reader.stop();
        }
        status = ConnectorStatus.STOPPED;
    }

    /**
     * Request the connector to start (resume).
     * Rebuilds the pipeline and starts a new reader thread.
     */
    public void requestStart() {
        if (status == ConnectorStatus.RUNNING || status == ConnectorStatus.STARTING) {
            LOG.warnf("Connector '%s' is already %s — ignoring start request", config.connector().id(), status);
            return;
        }
        LOG.infof("Start requested for connector '%s'", config.connector().id());
        onStart(null);
    }
}
