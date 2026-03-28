package com.paulsnow.qcdcf.postgres.watermark;

import java.time.Instant;
import java.util.UUID;

/**
 * A row in the watermark table representing a boundary marker.
 *
 * @param windowId  the watermark window this boundary belongs to
 * @param boundary  whether this is a LOW or HIGH boundary
 * @param timestamp when the boundary was written
 * @author Paul Snow
 * @since 0.0.0
 */
public record WatermarkBoundary(UUID windowId, Boundary boundary, Instant timestamp) {

    public enum Boundary {
        LOW, HIGH
    }
}
