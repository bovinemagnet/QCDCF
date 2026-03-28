package com.paulsnow.qcdcf.runtime.health;

import com.paulsnow.qcdcf.core.connector.ConnectorStatus;
import com.paulsnow.qcdcf.runtime.bootstrap.ConnectorBootstrap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

/**
 * Readiness health check based on the connector status.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Readiness
@ApplicationScoped
public class SourceHealthCheck implements HealthCheck {

    @Inject
    ConnectorBootstrap bootstrap;

    @Override
    public HealthCheckResponse call() {
        ConnectorStatus status = bootstrap.status();
        boolean ready = status == ConnectorStatus.RUNNING || status == ConnectorStatus.SNAPSHOTTING;
        return HealthCheckResponse.named("qcdcf-source")
                .status(ready)
                .withData("connectorId", bootstrap.connectorId())
                .withData("status", status.name())
                .build();
    }
}
