package com.paulsnow.qcdcf.runtime.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Typed configuration mapping for the CDC connector runtime.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ConfigMapping(prefix = "qcdcf")
public interface ConnectorRuntimeConfig {

    /** Connector configuration. */
    ConnectorConfig connector();

    /** Source database configuration. */
    SourceConfig source();

    /** Sink configuration. */
    SinkConfig sink();

    interface ConnectorConfig {
        @WithDefault("default-connector")
        String id();

        /** Whether to auto-start the WAL reader on application startup. */
        @WithDefault("true")
        boolean autoStart();
    }

    interface SourceConfig {
        @WithDefault("qcdcf_slot")
        String slotName();

        @WithDefault("qcdcf_pub")
        String publicationName();

        @WithDefault("1000")
        int chunkSize();
    }

    interface SinkConfig {
        @WithDefault("logging")
        String type();

        /** Kafka-specific sink configuration. */
        KafkaConfig kafka();

        interface KafkaConfig {
            @WithDefault("localhost:9092")
            String bootstrapServers();

            @WithDefault("qcdcf")
            String topicPrefix();
        }
    }
}
