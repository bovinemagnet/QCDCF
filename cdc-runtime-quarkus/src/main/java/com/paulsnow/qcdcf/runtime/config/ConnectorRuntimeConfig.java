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

    /** Resilience configuration. */
    ResilienceConfig resilience();

    /** Health check configuration. */
    HealthConfig health();

    /** Snapshot configuration. */
    SnapshotConfig snapshot();

    interface ConnectorConfig {
        @WithDefault("default-connector")
        String id();

        /** Whether to auto-start the WAL reader on application startup. */
        @WithDefault("true")
        boolean autoStart();

        @WithDefault("true")
        boolean validateOnStartup();
    }

    interface SourceConfig {
        @WithDefault("qcdcf_slot")
        String slotName();

        @WithDefault("qcdcf_pub")
        String publicationName();

        @WithDefault("1000")
        int chunkSize();
    }

    interface ResilienceConfig {
        WalRetryConfig walRetry();
        SnapshotRetryConfig snapshotRetry();
        SinkRetryConfig sinkRetry();
        CheckpointCbConfig checkpointCb();

        interface WalRetryConfig {
            @WithDefault("1s")
            String delay();
            @WithDefault("60s")
            String maxDelay();
            @WithDefault("200ms")
            String jitter();
        }

        interface SnapshotRetryConfig {
            @WithDefault("3")
            int maxRetries();
            @WithDefault("2s")
            String delay();
        }

        interface SinkRetryConfig {
            @WithDefault("3")
            int maxRetries();
            @WithDefault("500ms")
            String delay();
            @WithDefault("5s")
            String timeout();
        }

        interface CheckpointCbConfig {
            @WithDefault("5")
            int failureThreshold();
            @WithDefault("10s")
            String window();
            @WithDefault("30s")
            String halfOpenDelay();
        }
    }

    interface HealthConfig {
        @WithDefault("60s")
        String walLagThreshold();
        @WithDefault("10m")
        String sustainedFailureThreshold();
    }

    interface SnapshotConfig {
        @WithDefault("100000")
        int maxBufferSize();

        ScheduleConfig schedule();

        interface ScheduleConfig {
            @WithDefault("false")
            boolean enabled();

            @WithDefault("0 0 2 * * ?")
            String cron();
        }
    }

    interface SinkConfig {
        @WithDefault("logging")
        String type();

        /** Kafka-specific sink configuration. */
        KafkaConfig kafka();

        /** RabbitMQ-specific sink configuration. */
        RabbitMQConfig rabbitmq();

        interface KafkaConfig {
            @WithDefault("localhost:9092")
            String bootstrapServers();

            @WithDefault("qcdcf")
            String topicPrefix();
        }

        interface RabbitMQConfig {
            @WithDefault("localhost")
            String host();
            @WithDefault("5672")
            int port();
            @WithDefault("guest")
            String username();
            @WithDefault("guest")
            String password();
            @WithDefault("/")
            String virtualHost();
            @WithDefault("qcdcf")
            String exchangeName();
            @WithDefault("topic")
            String exchangeType();
        }
    }
}
