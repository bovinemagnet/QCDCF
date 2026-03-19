package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.LoggingEventSink;
import com.paulsnow.qcdcf.runtime.kafka.EventSerializer;
import com.paulsnow.qcdcf.runtime.kafka.KafkaEventSink;
import com.paulsnow.qcdcf.runtime.kafka.PartitionKeyStrategy;
import com.paulsnow.qcdcf.runtime.kafka.TopicRouter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * CDI producer for the {@link EventSink} used by the publish stage of the pipeline.
 * <p>
 * Selects the sink implementation based on the {@code qcdcf.sink.type} configuration property.
 * Supported values are {@code logging} (default) and {@code kafka}.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class EventSinkProducer {

    private static final Logger LOG = LoggerFactory.getLogger(EventSinkProducer.class);

    @ConfigProperty(name = "qcdcf.sink.type", defaultValue = "logging")
    String sinkType;

    @ConfigProperty(name = "qcdcf.sink.kafka.bootstrap-servers", defaultValue = "localhost:9092")
    String bootstrapServers;

    @ConfigProperty(name = "qcdcf.sink.kafka.topic-prefix", defaultValue = "qcdcf")
    String topicPrefix;

    /**
     * Produces the application-scoped {@link EventSink} instance.
     *
     * @return the configured event sink
     */
    @Produces
    @ApplicationScoped
    public EventSink createSink() {
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
        LOG.info("Using KafkaEventSink with bootstrap servers: {}", bootstrapServers);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("enable.idempotence", "true");

        return new KafkaEventSink(
                props,
                new TopicRouter(topicPrefix),
                new PartitionKeyStrategy(),
                new EventSerializer()
        );
    }
}
