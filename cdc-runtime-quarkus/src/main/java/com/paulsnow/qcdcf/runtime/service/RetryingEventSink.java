package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Decorator that retries failed publish attempts with exponential backoff.
 * <p>
 * Wraps any {@link EventSink} implementation and retries on {@link PublishResult.Failure}.
 * After all retries are exhausted, returns the last failure result.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class RetryingEventSink implements EventSink {

    private static final Logger LOG = LoggerFactory.getLogger(RetryingEventSink.class);

    private final EventSink delegate;
    private final int maxRetries;
    private final long initialDelayMs;

    public RetryingEventSink(EventSink delegate, int maxRetries, long initialDelayMs) {
        this.delegate = delegate;
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
    }

    @Override
    public PublishResult publish(ChangeEnvelope event) {
        PublishResult result = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            result = delegate.publish(event);
            if (result.isSuccess()) {
                return result;
            }
            LOG.warn("Publish attempt {}/{} failed for {}: {}",
                    attempt, maxRetries, event.tableId(), ((PublishResult.Failure) result).reason());
            if (attempt < maxRetries) {
                sleepWithBackoff(attempt);
            }
        }
        return result;
    }

    @Override
    public PublishResult publishBatch(List<ChangeEnvelope> events) {
        PublishResult result = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            result = delegate.publishBatch(events);
            if (result.isSuccess()) {
                return result;
            }
            LOG.warn("Batch publish attempt {}/{} failed: {}",
                    attempt, maxRetries, ((PublishResult.Failure) result).reason());
            if (attempt < maxRetries) {
                sleepWithBackoff(attempt);
            }
        }
        return result;
    }

    private void sleepWithBackoff(int attempt) {
        if (initialDelayMs <= 0) return;
        long delay = initialDelayMs * (1L << (attempt - 1));
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
