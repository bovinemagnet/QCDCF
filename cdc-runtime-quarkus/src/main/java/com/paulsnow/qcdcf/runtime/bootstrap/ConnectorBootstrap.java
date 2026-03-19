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
import com.paulsnow.qcdcf.runtime.messaging.WalIngressChannelBridge;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Bootstraps the CDC connector on application startup.
 * <p>
 * Creates the WAL capture pipeline components (replication client, decoder, normaliser)
 * and wires them through the {@link WalIngressChannelBridge} into the reactive messaging
 * channels. The blocking replication reader runs on a dedicated daemon thread.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class ConnectorBootstrap {

    private static final Logger LOG = Logger.getLogger(ConnectorBootstrap.class);

    private final ConnectorRuntimeConfig config;

    @Inject
    WalIngressChannelBridge bridge;

    @ConfigProperty(name = "quarkus.datasource.jdbc.url")
    String jdbcUrl;

    @ConfigProperty(name = "quarkus.datasource.username")
    String username;

    @ConfigProperty(name = "quarkus.datasource.password")
    String password;

    private volatile ConnectorStatus status = ConnectorStatus.STOPPED;
    private PostgresLogStreamReader reader;
    private Thread readerThread;

    public ConnectorBootstrap(ConnectorRuntimeConfig config) {
        this.config = config;
    }

    void onStart(@Observes StartupEvent event) {
        LOG.infof("QCDCF connector '%s' starting", config.connector().id());
        status = ConnectorStatus.STARTING;

        // Build the pipeline components
        PostgresLogicalReplicationClient client = new PostgresLogicalReplicationClient(
                jdbcUrl, username, password,
                config.source().slotName(),
                config.source().publicationName()
        );

        PgOutputMessageDecoder decoder = new PgOutputMessageDecoder();
        PgOutputEventNormaliser normaliser = new PgOutputEventNormaliser(config.connector().id());

        // Create a bridge sink that pushes events into the reactive messaging channel
        EventSink bridgeSink = new EventSink() {
            @Override
            public PublishResult publish(ChangeEnvelope event) {
                bridge.emit(event);
                return new PublishResult.Success(1);
            }

            @Override
            public PublishResult publishBatch(List<ChangeEnvelope> events) {
                events.forEach(bridge::emit);
                return new PublishResult.Success(events.size());
            }
        };

        reader = new PostgresLogStreamReader(client, decoder, normaliser, bridgeSink);

        // Start the blocking reader on a dedicated daemon thread
        readerThread = new Thread(() -> {
            try {
                reader.start(0);
            } catch (Exception e) {
                LOG.errorf(e, "WAL reader thread failed for connector '%s'", config.connector().id());
                status = ConnectorStatus.FAILED;
            }
        }, "qcdcf-wal-reader-" + config.connector().id());
        readerThread.setDaemon(true);
        readerThread.start();

        status = ConnectorStatus.RUNNING;
        LOG.infof("QCDCF connector '%s' is running", config.connector().id());
    }

    void onStop(@Observes ShutdownEvent event) {
        LOG.infof("QCDCF connector '%s' shutting down", config.connector().id());
        if (reader != null) {
            reader.stop();
        }
        if (readerThread != null) {
            readerThread.interrupt();
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
        if (readerThread != null) {
            readerThread.interrupt();
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
