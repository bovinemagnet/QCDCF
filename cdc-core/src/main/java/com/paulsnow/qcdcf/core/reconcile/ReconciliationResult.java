package com.paulsnow.qcdcf.core.reconcile;

import com.paulsnow.qcdcf.model.ChangeEnvelope;

import java.util.List;

/**
 * The result of reconciling log events with snapshot events within a watermark window.
 *
 * @param mergedEvents     the final ordered list of events after reconciliation
 * @param logEventsDropped number of log events removed due to key collision with snapshot data
 * @param snapshotEvents   total snapshot events in the window
 * @param logEvents        total log events in the window
 * @author Paul Snow
 * @since 0.0.0
 */
public record ReconciliationResult(
        List<ChangeEnvelope> mergedEvents,
        int logEventsDropped,
        int snapshotEvents,
        int logEvents
) {

    public ReconciliationResult {
        mergedEvents = List.copyOf(mergedEvents);
    }
}
