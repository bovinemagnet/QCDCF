package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.core.checkpoint.CheckpointManager;
import com.paulsnow.qcdcf.core.checkpoint.ConnectorCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wrapper around {@link CheckpointManager} with a simple circuit breaker pattern.
 * <p>
 * After {@code failureThreshold} consecutive failures, the circuit opens and
 * save operations are skipped for {@code cooldownSeconds}. This prevents
 * hammering a failing database with checkpoint writes.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class ResilientCheckpointRepository implements CheckpointManager {

    private static final Logger LOG = LoggerFactory.getLogger(ResilientCheckpointRepository.class);

    private final CheckpointManager delegate;
    private final int failureThreshold;
    private final long cooldownSeconds;

    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicReference<Instant> circuitOpenedAt = new AtomicReference<>(null);

    public ResilientCheckpointRepository(CheckpointManager delegate, int failureThreshold, long cooldownSeconds) {
        this.delegate = delegate;
        this.failureThreshold = failureThreshold;
        this.cooldownSeconds = cooldownSeconds;
    }

    @Override
    public void save(ConnectorCheckpoint checkpoint) {
        Instant openedAt = circuitOpenedAt.get();
        if (openedAt != null) {
            if (Instant.now().isBefore(openedAt.plusSeconds(cooldownSeconds))) {
                LOG.warn("Circuit breaker open — skipping checkpoint save for connector '{}'", checkpoint.connectorId());
                return;
            }
            LOG.info("Circuit breaker half-open — attempting checkpoint save");
            circuitOpenedAt.set(null);
            consecutiveFailures.set(0);
        }

        try {
            delegate.save(checkpoint);
            consecutiveFailures.set(0);
        } catch (Exception e) {
            int failures = consecutiveFailures.incrementAndGet();
            if (failures >= failureThreshold) {
                LOG.error("Checkpoint save failed {} times — opening circuit breaker for {}s",
                        failures, cooldownSeconds);
                circuitOpenedAt.set(Instant.now());
            }
            throw e;
        }
    }

    @Override
    public Optional<ConnectorCheckpoint> load(String connectorId) {
        return delegate.load(connectorId);
    }
}
