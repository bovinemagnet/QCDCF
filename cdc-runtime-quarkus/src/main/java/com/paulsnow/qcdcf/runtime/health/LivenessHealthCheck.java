package com.paulsnow.qcdcf.runtime.health;

import com.paulsnow.qcdcf.core.connector.ConnectorStatus;
import com.paulsnow.qcdcf.runtime.bootstrap.ConnectorBootstrap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

/**
 * Liveness health check that reports UP unless the connector has permanently failed.
 * <p>
 * A FAILED status indicates the connector cannot recover without intervention,
 * so liveness reports DOWN to allow container orchestrators to restart.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Liveness
@ApplicationScoped
public class LivenessHealthCheck implements HealthCheck {

    @Inject
    ConnectorBootstrap bootstrap;

    @Override
    public HealthCheckResponse call() {
        ConnectorStatus status = bootstrap.status();
        boolean alive = status != ConnectorStatus.FAILED;
        return HealthCheckResponse.named("qcdcf-liveness")
                .status(alive)
                .withData("status", status.name())
                .build();
    }
}
