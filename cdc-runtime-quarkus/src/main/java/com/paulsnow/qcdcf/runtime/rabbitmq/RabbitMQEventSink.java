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
 * Messages are published with persistent delivery mode.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class RabbitMQEventSink implements EventSink {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQEventSink.class);

    private final Channel channel;
    private final ExchangeRouter router;
    private final EventSerializer serialiser;
    private final String exchangeType;
    private volatile boolean exchangeDeclared;

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
            ensureExchangeDeclared();
            String exchange = router.exchangeName();
            String routingKey = router.routingKey(event);
            byte[] body = serialiser.serialise(event);
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .deliveryMode(2).contentType("application/json").build();
            channel.basicPublish(exchange, routingKey, props, body);
            LOG.debug("Published event {} to exchange {} with routing key {}", event.eventId(), exchange, routingKey);
            return new PublishResult.Success(1);
        } catch (Exception e) {
            LOG.error("RabbitMQ publish failed for event {}: {}", event.eventId(), e.getMessage(), e);
            return new PublishResult.Failure("RabbitMQ publish failed: " + e.getMessage(), e);
        }
    }

    @Override
    public PublishResult publishBatch(List<ChangeEnvelope> events) {
        for (ChangeEnvelope event : events) {
            PublishResult result = publish(event);
            if (!result.isSuccess()) return result;
        }
        return new PublishResult.Success(events.size());
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

    private void ensureExchangeDeclared() throws IOException {
        if (!exchangeDeclared) {
            channel.exchangeDeclare(router.exchangeName(), exchangeType, true);
            exchangeDeclared = true;
            LOG.info("Declared RabbitMQ exchange '{}' (type={})", router.exchangeName(), exchangeType);
        }
    }
}
