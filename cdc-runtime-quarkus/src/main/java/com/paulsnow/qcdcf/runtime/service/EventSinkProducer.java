package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.LoggingEventSink;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import com.paulsnow.qcdcf.runtime.kafka.EventSerializer;
import com.paulsnow.qcdcf.runtime.kafka.KafkaEventSink;
import com.paulsnow.qcdcf.runtime.kafka.PartitionKeyStrategy;
import com.paulsnow.qcdcf.runtime.kafka.TopicRouter;
import com.paulsnow.qcdcf.runtime.rabbitmq.ExchangeRouter;
import com.paulsnow.qcdcf.runtime.rabbitmq.RabbitMQEventSink;
import com.rabbitmq.client.ConnectionFactory;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * CDI producer for the {@link EventSink} used by the publish stage of the pipeline.
 * <p>
 * Selects the sink implementation based on {@link ConnectorRuntimeConfig.SinkConfig#type()}.
 * Supported values are {@code logging} (default), {@code kafka}, and {@code rabbitmq}.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class EventSinkProducer {

    private static final Logger LOG = LoggerFactory.getLogger(EventSinkProducer.class);

    @Inject
    ConnectorRuntimeConfig config;

    private EventSink createdSink;

    @Produces
    @ApplicationScoped
    public EventSink createSink() {
        String sinkType = config.sink().type();
        EventSink sink = switch (sinkType) {
            case "kafka" -> createKafkaSink();
            case "rabbitmq" -> createRabbitMQSink();
            case "logging" -> {
                LOG.info("Using LoggingEventSink");
                yield new LoggingEventSink();
            }
            default -> {
                LOG.warn("Unknown sink type '{}', falling back to LoggingEventSink", sinkType);
                yield new LoggingEventSink();
            }
        };

        int maxRetries = config.resilience().sinkRetry().maxRetries();
        String delayStr = config.resilience().sinkRetry().delay();
        long delayMs = parseDelayMs(delayStr);
        EventSink retrying = new RetryingEventSink(sink, maxRetries, delayMs);
        LOG.info("Wrapped sink with RetryingEventSink (maxRetries={}, delay={})", maxRetries, delayStr);

        createdSink = retrying;
        return retrying;
    }

    @PreDestroy
    void cleanup() {
        if (createdSink != null) {
            try {
                createdSink.close();
            } catch (Exception e) {
                LOG.warn("Error closing EventSink: {}", e.getMessage());
            }
        }
    }

    private static long parseDelayMs(String delay) {
        delay = delay.trim().toLowerCase();
        if (delay.endsWith("ms")) return Long.parseLong(delay.replace("ms", ""));
        if (delay.endsWith("s")) return (long) (Double.parseDouble(delay.replace("s", "")) * 1000);
        return Long.parseLong(delay);
    }

    private RabbitMQEventSink createRabbitMQSink() {
        var rmqConfig = config.sink().rabbitmq();
        LOG.info("Using RabbitMQEventSink with host: {}:{}", rmqConfig.host(), rmqConfig.port());
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(rmqConfig.host());
            factory.setPort(rmqConfig.port());
            factory.setUsername(rmqConfig.username());
            factory.setPassword(rmqConfig.password());
            factory.setVirtualHost(rmqConfig.virtualHost());
            var connection = factory.newConnection();
            var channel = connection.createChannel();
            return new RabbitMQEventSink(channel, new ExchangeRouter(rmqConfig.exchangeName()),
                    new EventSerializer(), rmqConfig.exchangeType());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create RabbitMQ connection: " + e.getMessage(), e);
        }
    }

    private KafkaEventSink createKafkaSink() {
        var kafkaConfig = config.sink().kafka();
        LOG.info("Using KafkaEventSink with bootstrap servers: {}", kafkaConfig.bootstrapServers());

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.bootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("enable.idempotence", "true");

        return new KafkaEventSink(
                props,
                new TopicRouter(kafkaConfig.topicPrefix()),
                new PartitionKeyStrategy(),
                new EventSerializer()
        );
    }
}
