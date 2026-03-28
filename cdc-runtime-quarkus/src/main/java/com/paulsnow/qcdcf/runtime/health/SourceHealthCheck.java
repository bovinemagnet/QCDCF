package com.paulsnow.qcdcf.runtime.health;

import com.paulsnow.qcdcf.core.connector.ConnectorStatus;
import com.paulsnow.qcdcf.runtime.bootstrap.ConnectorBootstrap;
import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

import java.time.Duration;
import java.time.Instant;

/**
 * Readiness health check based on the connector status and sustained failure detection.
 * <p>
 * Reports DOWN if the connector is not in a running state or if the last successful
 * connection exceeds the configured sustained failure threshold.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Readiness
@ApplicationScoped
public class SourceHealthCheck implements HealthCheck {

    @Inject
    ConnectorBootstrap bootstrap;

    @Inject
    ConnectorRuntimeConfig config;

    @Override
    public HealthCheckResponse call() {
        ConnectorStatus status = bootstrap.status();
        boolean ready = status == ConnectorStatus.RUNNING || status == ConnectorStatus.SNAPSHOTTING;

        Instant lastConn = bootstrap.lastSuccessfulConnection();
        String thresholdStr = config.health().sustainedFailureThreshold();
        Duration threshold = parseDuration(thresholdStr);
        boolean sustainedFailure = lastConn != null &&
                Duration.between(lastConn, Instant.now()).compareTo(threshold) > 0;

        return HealthCheckResponse.named("qcdcf-source")
                .status(ready && !sustainedFailure)
                .withData("connectorId", bootstrap.connectorId())
                .withData("status", status.name())
                .withData("lastSuccessfulConnection", lastConn != null ? lastConn.toString() : "never")
                .build();
    }

    private static Duration parseDuration(String value) {
        value = value.trim().toLowerCase();
        if (value.endsWith("ms")) return Duration.ofMillis(Long.parseLong(value.replace("ms", "")));
        if (value.endsWith("m")) return Duration.ofMinutes(Long.parseLong(value.replace("m", "")));
        if (value.endsWith("s")) return Duration.ofSeconds(Long.parseLong(value.replace("s", "")));
        return Duration.ofSeconds(Long.parseLong(value));
    }
}
