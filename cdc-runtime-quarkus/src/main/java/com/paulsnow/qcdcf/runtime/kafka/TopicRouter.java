package com.paulsnow.qcdcf.runtime.kafka;

import com.paulsnow.qcdcf.model.ChangeEnvelope;

/**
 * Determines the Kafka topic for a given change event.
 * <p>
 * Default strategy: one topic per table, named {@code {prefix}.{schema}.{table}}.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class TopicRouter {

    private final String topicPrefix;

    public TopicRouter(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    /**
     * Routes the given event to a Kafka topic.
     *
     * @param event the change event
     * @return the target topic name
     */
    public String route(ChangeEnvelope event) {
        return topicPrefix + "." + event.tableId().canonicalName();
    }
}
