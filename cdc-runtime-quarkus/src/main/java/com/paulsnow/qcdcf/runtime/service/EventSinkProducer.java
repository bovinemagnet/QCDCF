package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.LoggingEventSink;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import com.paulsnow.qcdcf.runtime.kafka.EventSerializer;
import com.paulsnow.qcdcf.runtime.kafka.KafkaEventSink;
import com.paulsnow.qcdcf.runtime.kafka.PartitionKeyStrategy;
import com.paulsnow.qcdcf.runtime.kafka.TopicRouter;
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
 * Supported values are {@code logging} (default) and {@code kafka}.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class EventSinkProducer {

    private static final Logger LOG = LoggerFactory.getLogger(EventSinkProducer.class);

    @Inject
    ConnectorRuntimeConfig config;

    @Produces
    @ApplicationScoped
    public EventSink createSink() {
        String sinkType = config.sink().type();
        return switch (sinkType) {
            case "kafka" -> createKafkaSink();
            case "logging" -> {
                LOG.info("Using LoggingEventSink");
                yield new LoggingEventSink();
            }
            default -> {
                LOG.warn("Unknown sink type '{}', falling back to LoggingEventSink", sinkType);
                yield new LoggingEventSink();
            }
        };
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
