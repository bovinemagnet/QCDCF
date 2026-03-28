package com.paulsnow.qcdcf.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Context for a watermark window used during snapshot reconciliation.
 * <p>
 * A watermark window brackets a snapshot chunk read. Events arriving
 * between the low and high marks must be reconciled against snapshot data
 * to eliminate duplicates.
 *
 * @param windowId  unique identifier for this watermark window
 * @param lowMark   timestamp of the low watermark write
 * @param highMark  timestamp of the high watermark write (null if window is still open)
 * @author Paul Snow
 * @since 0.0.0
 */
public record WatermarkContext(UUID windowId, Instant lowMark, Instant highMark) {

    public WatermarkContext {
        Objects.requireNonNull(windowId, "windowId must not be null");
        Objects.requireNonNull(lowMark, "lowMark must not be null");
    }

    /**
     * Returns {@code true} if this window has been closed (high watermark written).
     */
    public boolean isClosed() {
        return highMark != null;
    }
}
