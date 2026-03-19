package com.paulsnow.qcdcf.postgres.replication;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Orchestrates the full CDC pipeline: replication client → decoder → normaliser → sink.
 * <p>
 * Reads raw WAL messages via {@link PostgresLogicalReplicationClient}, decodes them
 * with {@link PgOutputMessageDecoder}, normalises data changes into {@link ChangeEnvelope}
 * via {@link PgOutputEventNormaliser}, and publishes them to an {@link EventSink}.
 * <p>
 * Transaction envelope messages (BEGIN/COMMIT) and relation metadata messages are handled
 * internally. The decoder's relation cache provides key column information for normalisation,
 * so no separate metadata cache is needed.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PostgresLogStreamReader {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresLogStreamReader.class);

    private final PostgresLogicalReplicationClient client;
    private final PgOutputMessageDecoder decoder;
    private final PgOutputEventNormaliser normaliser;
    private final EventSink sink;

    private volatile boolean running;
    private long lastProcessedLsn;
    private Long currentTxId;

    /**
     * Creates a new log stream reader wiring all pipeline components.
     *
     * @param client     the replication client for reading WAL messages
     * @param decoder    the pgoutput binary protocol decoder
     * @param normaliser the event normaliser converting decoded messages to ChangeEnvelopes
     * @param sink       the event sink to publish normalised events to
     */
    public PostgresLogStreamReader(PostgresLogicalReplicationClient client,
                                   PgOutputMessageDecoder decoder,
                                   PgOutputEventNormaliser normaliser,
                                   EventSink sink) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.decoder = Objects.requireNonNull(decoder, "decoder must not be null");
        this.normaliser = Objects.requireNonNull(normaliser, "normaliser must not be null");
        this.sink = Objects.requireNonNull(sink, "sink must not be null");
    }

    /**
     * Starts the WAL capture pipeline from the given LSN.
     * <p>
     * This method <strong>blocks</strong> the calling thread. Messages are processed
     * in commit order: BEGIN → data changes → COMMIT. LSN acknowledgement occurs
     * after successful publication of each data change, and after each COMMIT.
     *
     * @param startLsn the LSN to begin streaming from (0 to use the slot's confirmed position)
     */
    public void start(long startLsn) {
        running = true;
        LOG.info("Starting WAL capture pipeline from LSN {}", startLsn);
        client.start(startLsn, this::handleRawMessage);
    }

    /**
     * Signals the pipeline to stop.
     */
    public void stop() {
        running = false;
        client.stop();
        LOG.info("WAL capture pipeline stopped");
    }

    /**
     * Returns the last successfully processed LSN.
     */
    public long lastProcessedLsn() {
        return lastProcessedLsn;
    }

    public boolean isRunning() {
        return running;
    }

    private void handleRawMessage(RawReplicationMessage raw) {
        DecodedReplicationMessage decoded = decoder.decode(raw);
        if (decoded == null) {
            return;  // unrecognised or relation metadata message
        }

        switch (decoded.type()) {
            case BEGIN -> {
                currentTxId = decoded.txId();
                LOG.trace("Transaction begin: txId={}", currentTxId);
            }
            case COMMIT -> {
                lastProcessedLsn = decoded.lsn();
                client.acknowledgeLsn(lastProcessedLsn);
                LOG.trace("Transaction commit acknowledged: LSN={}", lastProcessedLsn);
                currentTxId = null;
            }
            case INSERT, UPDATE, DELETE -> {
                DecodedReplicationMessage enriched = enrichWithTxId(decoded);
                ChangeEnvelope envelope = normaliser.normalise(enriched);

                PublishResult result = sink.publish(envelope);
                if (result.isSuccess()) {
                    lastProcessedLsn = raw.lsn();
                    LOG.debug("Published {} event for {} at LSN {}",
                            decoded.operation(), decoded.tableId(), raw.lsn());
                } else {
                    var failure = (PublishResult.Failure) result;
                    LOG.error("Failed to publish event for {} at LSN {}: {}",
                            decoded.tableId(), raw.lsn(), failure.reason());
                }
            }
        }
    }

    private DecodedReplicationMessage enrichWithTxId(DecodedReplicationMessage msg) {
        if (currentTxId != null && msg.txId() == null) {
            return new DecodedReplicationMessage(
                    msg.type(), msg.lsn(), currentTxId, msg.commitTimestamp(),
                    msg.tableId(), msg.operation(), msg.oldValues(), msg.newValues(),
                    msg.keyColumns()
            );
        }
        return msg;
    }
}
