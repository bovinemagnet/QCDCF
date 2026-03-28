package com.paulsnow.qcdcf.runtime.kafka;

import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.core.sink.PublishResult;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Publishes change events to Apache Kafka.
 * <p>
 * Uses the Kafka producer client directly for synchronous, at-least-once delivery.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class KafkaEventSink implements EventSink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEventSink.class);

    private final KafkaProducer<String, byte[]> producer;
    private final TopicRouter topicRouter;
    private final PartitionKeyStrategy partitionKeyStrategy;
    private final EventSerializer serialiser;

    /**
     * Creates a new Kafka event sink.
     *
     * @param producerProperties Kafka producer configuration properties
     * @param topicRouter        determines the target topic for each event
     * @param partitionKeyStrategy determines the partition key for each event
     * @param serialiser         serialises events to bytes
     */
    public KafkaEventSink(Properties producerProperties,
                          TopicRouter topicRouter,
                          PartitionKeyStrategy partitionKeyStrategy,
                          EventSerializer serialiser) {
        this.producer = new KafkaProducer<>(producerProperties);
        this.topicRouter = topicRouter;
        this.partitionKeyStrategy = partitionKeyStrategy;
        this.serialiser = serialiser;
    }

    @Override
    public PublishResult publish(ChangeEnvelope event) {
        String topic = topicRouter.route(event);
        String key = partitionKeyStrategy.partitionKey(event);
        byte[] value = serialiser.serialise(event);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
        try {
            producer.send(record).get(); // synchronous for at-least-once
            LOG.debug("Published event {} to topic {} with key {}", event.eventId(), topic, key);
            return new PublishResult.Success(1);
        } catch (Exception e) {
            LOG.error("Kafka publish failed for event {} on topic {}", event.eventId(), topic, e);
            return new PublishResult.Failure("Kafka publish failed: " + e.getMessage(), e);
        }
    }

    @Override
    public PublishResult publishBatch(List<ChangeEnvelope> events) {
        for (ChangeEnvelope event : events) {
            PublishResult result = publish(event);
            if (!result.isSuccess()) {
                return result;
            }
        }
        return new PublishResult.Success(events.size());
    }

    /**
     * Closes the underlying Kafka producer, releasing all resources.
     */
    @Override
    public void close() {
        LOG.info("Closing Kafka producer");
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
    }
}
