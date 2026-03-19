package com.paulsnow.qcdcf.core.sink;

import com.paulsnow.qcdcf.model.ChangeEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An event sink that logs each event via SLF4J. Useful for development and debugging.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class LoggingEventSink implements EventSink {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingEventSink.class);

    @Override
    public PublishResult publish(ChangeEnvelope event) {
        LOG.info("CDC event: {} {} on {} key={}",
                event.captureMode(), event.operation(),
                event.tableId(), event.key().toCanonical());
        return new PublishResult.Success(1);
    }

    @Override
    public PublishResult publishBatch(List<ChangeEnvelope> events) {
        events.forEach(this::publish);
        return new PublishResult.Success(events.size());
    }
}
