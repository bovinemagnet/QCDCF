package com.paulsnow.qcdcf.runtime.health;

import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

/**
 * Readiness health check for the configured event sink.
 * <p>
 * For Kafka sinks, performs a DNS lookup on the first bootstrap server
 * to verify basic reachability.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Readiness
@ApplicationScoped
public class SinkHealthCheck implements HealthCheck {

    @Inject
    ConnectorRuntimeConfig config;

    @Override
    public HealthCheckResponse call() {
        String sinkType = config.sink().type();
        boolean reachable = true;

        if ("kafka".equals(sinkType)) {
            try {
                String servers = config.sink().kafka().bootstrapServers();
                String host = servers.split(",")[0].split(":")[0];
                java.net.InetAddress.getByName(host);
            } catch (Exception e) {
                reachable = false;
            }
        }

        return HealthCheckResponse.named("qcdcf-sink")
                .status(reachable)
                .withData("type", sinkType)
                .build();
    }
}
