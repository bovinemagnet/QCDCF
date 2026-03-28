package com.paulsnow.qcdcf.core.reconcile;

import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.core.watermark.WatermarkWindow;

import java.util.List;

/**
 * Reconciles log events and snapshot events within a watermark window.
 * <p>
 * The core insight from Netflix DBLog: when a snapshot chunk is read between
 * low and high watermarks, any log events for the same keys must be removed
 * in favour of the snapshot data, since the snapshot provides a more recent
 * consistent view.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public interface ReconciliationEngine {

    /**
     * Reconciles log and snapshot events for a given watermark window.
     *
     * @param window         the watermark window that brackets this reconciliation
     * @param logEvents      log events captured during the window
     * @param snapshotEvents snapshot events from the chunk read
     * @return the reconciliation result with merged events
     */
    ReconciliationResult reconcile(
            WatermarkWindow window,
            List<ChangeEnvelope> logEvents,
            List<ChangeEnvelope> snapshotEvents
    );
}
