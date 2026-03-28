package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.InMemoryEventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class RetryingEventSinkTest {

    @Test
    void retriesOnFailureThenSucceeds() {
        AtomicInteger attempts = new AtomicInteger(0);
        EventSink delegate = new InMemoryEventSink() {
            @Override
            public PublishResult publish(ChangeEnvelope event) {
                if (attempts.incrementAndGet() <= 2) {
                    return new PublishResult.Failure("transient error");
                }
                return new PublishResult.Success(1);
            }
        };

        var retrying = new RetryingEventSink(delegate, 3, 0);
        PublishResult result = retrying.publish(sampleEvent());

        assertThat(result.isSuccess()).isTrue();
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void returnsFailureAfterMaxRetries() {
        EventSink delegate = new InMemoryEventSink() {
            @Override
            public PublishResult publish(ChangeEnvelope event) {
                return new PublishResult.Failure("permanent error");
            }
        };

        var retrying = new RetryingEventSink(delegate, 3, 0);
        PublishResult result = retrying.publish(sampleEvent());

        assertThat(result.isSuccess()).isFalse();
    }

    private ChangeEnvelope sampleEvent() {
        return new ChangeEnvelope(
                UUID.randomUUID(), "test", new TableId("public", "customer"),
                OperationType.INSERT, CaptureMode.LOG,
                new RowKey(Map.of("id", 1)), null, Map.of("id", 1),
                new SourcePosition(1000L, Instant.now()), Instant.now(), null, Map.of());
    }
}
