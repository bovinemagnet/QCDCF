package com.paulsnow.qcdcf.core.reconcile;

import com.paulsnow.qcdcf.model.ChangeEnvelope;
import com.paulsnow.qcdcf.core.watermark.WatermarkWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default reconciliation engine implementing key-collision removal.
 * <p>
 * For a given watermark window:
 * <ol>
 *   <li>Collect the set of keys present in the snapshot chunk</li>
 *   <li>Remove any log events whose keys collide with snapshot keys</li>
 *   <li>Emit the remaining log events followed by the snapshot events</li>
 * </ol>
 * <p>
 * Log events outside the window are passed through unchanged.
 * Snapshot events replace log events for the same key because the
 * snapshot read under READ COMMITTED provides a consistent view.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class DefaultReconciliationEngine implements ReconciliationEngine {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultReconciliationEngine.class);

    @Override
    public ReconciliationResult reconcile(
            WatermarkWindow window,
            List<ChangeEnvelope> logEvents,
            List<ChangeEnvelope> snapshotEvents
    ) {
        // Collect canonical keys from snapshot events
        Set<String> snapshotKeys = snapshotEvents.stream()
                .map(e -> e.key().toCanonical())
                .collect(Collectors.toSet());

        // Partition log events: keep those without key collisions
        List<ChangeEnvelope> survivingLogEvents = new ArrayList<>();
        int dropped = 0;

        for (ChangeEnvelope logEvent : logEvents) {
            if (snapshotKeys.contains(logEvent.key().toCanonical())) {
                dropped++;
                LOG.debug("Dropping log event for key {} — superseded by snapshot in window {}",
                        logEvent.key().toCanonical(), window.windowId());
            } else {
                survivingLogEvents.add(logEvent);
            }
        }

        // Merge: surviving log events first, then snapshot events
        List<ChangeEnvelope> merged = new ArrayList<>(survivingLogEvents.size() + snapshotEvents.size());
        merged.addAll(survivingLogEvents);
        merged.addAll(snapshotEvents);

        LOG.info("Reconciliation for window {}: {} log events, {} snapshot events, {} dropped",
                window.windowId(), logEvents.size(), snapshotEvents.size(), dropped);

        return new ReconciliationResult(merged, dropped, snapshotEvents.size(), logEvents.size());
    }
}
