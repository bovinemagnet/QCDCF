package com.paulsnow.qcdcf.runtime.rabbitmq;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.runtime.kafka.EventSerializer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;

/**
 * Publishes change events to a RabbitMQ exchange.
 * <p>
 * Uses the AMQP client directly for synchronous, at-least-once delivery.
 * Messages are published with persistent delivery mode (deliveryMode=2) and
 * publisher confirms are enabled: each {@link #publish} blocks until the broker
 * acknowledges the message, returning {@link PublishResult.Failure} on a nack
 * or confirm timeout so the caller can retry rather than silently lose data.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class RabbitMQEventSink implements EventSink {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQEventSink.class);

    /** Maximum time to wait for a broker publisher confirm before failing. */
    private static final long CONFIRM_TIMEOUT_MS = 5_000L;

    /** Persistent (deliveryMode=2) JSON message properties — immutable, shared across publishes. */
    private static final AMQP.BasicProperties PERSISTENT_JSON =
            new AMQP.BasicProperties.Builder().deliveryMode(2).contentType("application/json").build();

    private final Channel channel;
    private final ExchangeRouter router;
    private final EventSerializer serialiser;
    private final String exchangeType;
    private volatile boolean ready;

    public RabbitMQEventSink(Channel channel, ExchangeRouter router,
                             EventSerializer serialiser, String exchangeType) {
        this.channel = channel;
        this.router = router;
        this.serialiser = serialiser;
        this.exchangeType = exchangeType;
    }

    @Override
    public PublishResult publish(ChangeEnvelope event) {
        try {
            ensureReady();
            String exchange = router.exchangeName();
            String routingKey = router.routingKey(event);
            byte[] body = serialiser.serialise(event);
            channel.basicPublish(exchange, routingKey, PERSISTENT_JSON, body);
            if (!channel.waitForConfirms(CONFIRM_TIMEOUT_MS)) {
                LOG.error("RabbitMQ broker nacked event {} (exchange={}, routingKey={})",
                        event.eventId(), exchange, routingKey);
                return new PublishResult.Failure("RabbitMQ broker rejected (nack) event " + event.eventId());
            }
            LOG.debug("Published event {} to exchange {} with routing key {}", event.eventId(), exchange, routingKey);
            return new PublishResult.Success(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted awaiting RabbitMQ confirm for event {}", event.eventId(), e);
            return new PublishResult.Failure("Interrupted awaiting RabbitMQ confirm: " + e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("RabbitMQ publish failed for event {}: {}", event.eventId(), e.getMessage(), e);
            return new PublishResult.Failure("RabbitMQ publish failed: " + e.getMessage(), e);
        }
    }

    @Override
    public PublishResult publishBatch(List<ChangeEnvelope> events) {
        if (events.isEmpty()) {
            return new PublishResult.Success(0);
        }
        try {
            ensureReady();
            for (ChangeEnvelope event : events) {
                channel.basicPublish(router.exchangeName(), router.routingKey(event),
                        PERSISTENT_JSON, serialiser.serialise(event));
            }
            // One confirm round-trip covers all messages published on this channel so far.
            if (!channel.waitForConfirms(CONFIRM_TIMEOUT_MS)) {
                LOG.error("RabbitMQ broker rejected one or more events in batch of {}", events.size());
                return new PublishResult.Failure(
                        "RabbitMQ broker rejected one or more events in batch of " + events.size());
            }
            return new PublishResult.Success(events.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted awaiting RabbitMQ confirms for batch of {}", events.size(), e);
            return new PublishResult.Failure("Interrupted awaiting RabbitMQ confirms: " + e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("RabbitMQ batch publish failed: {}", e.getMessage(), e);
            return new PublishResult.Failure("RabbitMQ batch publish failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        LOG.info("Closing RabbitMQ channel and connection");
        try {
            if (channel != null && channel.isOpen()) channel.close();
            if (channel != null && channel.getConnection() != null && channel.getConnection().isOpen())
                channel.getConnection().close();
        } catch (Exception e) {
            LOG.warn("Error closing RabbitMQ resources: {}", e.getMessage());
        }
    }

    private void ensureReady() throws IOException {
        if (!ready) {
            channel.exchangeDeclare(router.exchangeName(), exchangeType, true);
            channel.confirmSelect();
            ready = true;
            LOG.info("Declared RabbitMQ exchange '{}' (type={}) and enabled publisher confirms",
                    router.exchangeName(), exchangeType);
        }
    }
}
