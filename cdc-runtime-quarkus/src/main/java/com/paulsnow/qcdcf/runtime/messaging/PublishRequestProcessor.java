package com.paulsnow.qcdcf.runtime.messaging;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.runtime.service.MetricsService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

/**
 * Consumes normalised change events from the {@code wal-normalised} channel and
 * publishes them to the configured {@link EventSink}.
 * <p>
 * This is the final stage of the reactive pipeline before delivery to the downstream
 * system. Currently delegates to a {@link com.paulsnow.qcdcf.core.sink.LoggingEventSink};
 * a Kafka sink will be wired in a future phase.
 * <p>
 * After successful publication, the event is recorded in the {@link MetricsService}
 * for dashboard display.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class PublishRequestProcessor {

    private static final Logger LOG = Logger.getLogger(PublishRequestProcessor.class);

    @Inject
    EventSink sink;

    @Inject
    MetricsService metricsService;

    /**
     * Processes a single change envelope received from the messaging channel.
     *
     * @param envelope the change event to publish
     */
    @Incoming("wal-normalised")
    public void process(ChangeEnvelope envelope) {
        LOG.debugf("Processing %s event for %s via sink",
                envelope.operation(), envelope.tableId());

        PublishResult result = sink.publish(envelope);
        if (result.isSuccess()) {
            metricsService.recordEvent(envelope);
        } else {
            var failure = (PublishResult.Failure) result;
            LOG.errorf("Sink publication failed for %s at position %s: %s",
                    envelope.tableId(), envelope.position(), failure.reason());
            metricsService.recordError(String.format("Sink publication failed for %s: %s",
                    envelope.tableId(), failure.reason()));
        }
    }
}
