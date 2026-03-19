package com.paulsnow.qcdcf.runtime.messaging;

import com.paulsnow.qcdcf.model.ChangeEnvelope;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.jboss.logging.Logger;

/**
 * Bridges the blocking WAL replication thread into the reactive messaging pipeline.
 * <p>
 * The {@link com.paulsnow.qcdcf.postgres.replication.PostgresLogStreamReader} runs on a
 * dedicated thread and produces {@link ChangeEnvelope} events synchronously. This bean
 * accepts those events and emits them onto the {@code wal-normalised} SmallRye Reactive
 * Messaging channel for downstream processing.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class WalIngressChannelBridge {

    private static final Logger LOG = Logger.getLogger(WalIngressChannelBridge.class);

    @Channel("wal-normalised")
    MutinyEmitter<ChangeEnvelope> emitter;

    /**
     * Emits a normalised change envelope onto the {@code wal-normalised} channel.
     * <p>
     * This method is called from the WAL reader pipeline thread. The emitter handles
     * back-pressure between the blocking producer and the reactive consumer.
     *
     * @param envelope the normalised change event to emit
     */
    public void emit(ChangeEnvelope envelope) {
        LOG.debugf("Emitting %s event for %s to wal-normalised channel",
                envelope.operation(), envelope.tableId());
        emitter.send(envelope).subscribe().with(
                success -> LOG.tracef("Event emitted for %s", envelope.tableId()),
                failure -> LOG.errorf(failure, "Failed to emit event for %s", envelope.tableId())
        );
    }
}
