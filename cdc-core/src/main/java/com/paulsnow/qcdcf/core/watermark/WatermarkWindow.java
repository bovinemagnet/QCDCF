package com.paulsnow.qcdcf.core.watermark;

import com.paulsnow.qcdcf.model.TableId;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Tracks the state of a single watermark window during snapshot reconciliation.
 *
 * @param windowId   unique identifier for this window
 * @param tableId    the table being snapshotted
 * @param lowMark    timestamp when the low watermark was written
 * @param highMark   timestamp when the high watermark was written (null if still open)
 * @param chunkIndex the snapshot chunk index this window covers
 * @author Paul Snow
 * @since 0.0.0
 */
public record WatermarkWindow(
        UUID windowId,
        TableId tableId,
        Instant lowMark,
        Instant highMark,
        int chunkIndex
) {

    public WatermarkWindow {
        Objects.requireNonNull(windowId, "windowId must not be null");
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(lowMark, "lowMark must not be null");
    }

    /** Returns {@code true} if the high watermark has been written. */
    public boolean isClosed() {
        return highMark != null;
    }

    /** Returns a new window with the high watermark set. */
    public WatermarkWindow close(Instant highMark) {
        Objects.requireNonNull(highMark, "highMark must not be null");
        return new WatermarkWindow(windowId, tableId, lowMark, highMark, chunkIndex);
    }
}
