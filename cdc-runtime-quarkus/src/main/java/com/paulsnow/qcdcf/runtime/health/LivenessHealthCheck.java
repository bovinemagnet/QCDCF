package com.paulsnow.qcdcf.runtime.health;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

/**
 * Liveness health check that reports UP when the application is running.
 * <p>
 * This is a simple liveness probe — if the application process is alive
 * and able to serve requests, it reports healthy.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@Liveness
@ApplicationScoped
public class LivenessHealthCheck implements HealthCheck {

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("qcdcf-liveness")
                .up()
                .build();
    }
}
