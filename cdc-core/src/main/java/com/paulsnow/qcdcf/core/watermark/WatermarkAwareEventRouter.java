package com.paulsnow.qcdcf.core.watermark;

import com.paulsnow.qcdcf.core.reconcile.ReconciliationEngine;
import com.paulsnow.qcdcf.core.reconcile.ReconciliationResult;
import com.paulsnow.qcdcf.core.sink.EventSink;
import com.paulsnow.qcdcf.model.CaptureMode;
import com.paulsnow.qcdcf.model.ChangeEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Routes events through the reconciliation engine when a watermark window is active.
 * <p>
 * When no snapshot is in progress, events pass through directly to the sink.
 * When a watermark window is open:
 * <ol>
 *   <li>LOG events are buffered</li>
 *   <li>When snapshot chunk data arrives (via {@link #onSnapshotChunk}), it is stored</li>
 *   <li>When the window closes (via {@link #closeWindow}), the reconciliation engine
 *       merges buffered log events with snapshot data, removing key collisions</li>
 *   <li>The merged result is emitted to the sink</li>
 * </ol>
 * <strong>Thread safety:</strong> This class is NOT thread-safe. It must only be called
 * from the WAL reader thread. All methods assume single-threaded access.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class WatermarkAwareEventRouter {

    private static final Logger LOG = LoggerFactory.getLogger(WatermarkAwareEventRouter.class);

    private final ReconciliationEngine reconciliationEngine;
    private final EventSink sink;
    private final int maxBufferSize;

    // Active window state
    private WatermarkWindow activeWindow;
    private final List<ChangeEnvelope> bufferedLogEvents = new ArrayList<>();
    private List<ChangeEnvelope> snapshotChunkEvents = List.of();

    public WatermarkAwareEventRouter(ReconciliationEngine reconciliationEngine, EventSink sink) {
        this(reconciliationEngine, sink, 100_000);
    }

    public WatermarkAwareEventRouter(ReconciliationEngine reconciliationEngine, EventSink sink, int maxBufferSize) {
        this.reconciliationEngine = reconciliationEngine;
        this.sink = sink;
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Processes a single event from the WAL stream.
     * <p>
     * If no window is active, the event passes through directly to the sink.
     * If a window is active and this is a LOG event, it is buffered for reconciliation.
     *
     * @param event the change event to process
     */
    public void onEvent(ChangeEnvelope event) {
        if (activeWindow == null) {
            // No active window — pass through directly
            sink.publish(event);
            return;
        }

        // Window is open — buffer LOG events for reconciliation
        if (event.captureMode() == CaptureMode.LOG) {
            if (bufferedLogEvents.size() >= maxBufferSize) {
                LOG.error("Watermark buffer overflow ({} events) — cancelling window for table {}",
                        maxBufferSize, activeWindow.tableId());
                cancelWindow(activeWindow);
                throw new IllegalStateException("Watermark buffer overflow: " + maxBufferSize + " events");
            }
            bufferedLogEvents.add(event);
            LOG.debug("Buffered LOG event for key {} in window {}",
                    event.key().toCanonical(), activeWindow.windowId());
        } else {
            // Non-LOG events (e.g. control events) pass through
            sink.publish(event);
        }
    }

    /**
     * Opens a watermark window. All subsequent LOG events will be buffered
     * until the window is closed.
     *
     * @param window the watermark window to open
     */
    public void openWindow(WatermarkWindow window) {
        if (activeWindow != null) {
            LOG.warn("Opening new window {} while window {} is still active — forcing close",
                    window.windowId(), activeWindow.windowId());
            closeWindow();
        }
        activeWindow = window;
        bufferedLogEvents.clear();
        snapshotChunkEvents = List.of();
        LOG.info("Opened reconciliation window {} for table {} chunk {}",
                window.windowId(), window.tableId(), window.chunkIndex());
    }

    /**
     * Provides the snapshot chunk data for the active window.
     *
     * @param events the snapshot row events from the chunk read
     */
    public void onSnapshotChunk(List<ChangeEnvelope> events) {
        if (activeWindow == null) {
            LOG.warn("Received snapshot chunk data but no window is active — publishing directly");
            events.forEach(sink::publish);
            return;
        }
        snapshotChunkEvents = new ArrayList<>(events);
        LOG.debug("Received {} snapshot events for window {}", events.size(), activeWindow.windowId());
    }

    /**
     * Closes the active window and runs reconciliation.
     * <p>
     * The reconciliation engine merges the buffered log events with the snapshot
     * chunk data, removing log events whose keys collide with snapshot rows.
     * The merged result is then published to the sink.
     */
    public void closeWindow() {
        if (activeWindow == null) {
            LOG.warn("No active window to close");
            return;
        }

        LOG.info("Closing window {} — reconciling {} log events with {} snapshot events",
                activeWindow.windowId(), bufferedLogEvents.size(), snapshotChunkEvents.size());

        ReconciliationResult result = reconciliationEngine.reconcile(
                activeWindow, bufferedLogEvents, snapshotChunkEvents);

        // Publish merged events to the sink
        result.mergedEvents().forEach(sink::publish);

        LOG.info("Window {} reconciliation complete: {} merged events, {} log events dropped",
                activeWindow.windowId(), result.mergedEvents().size(), result.logEventsDropped());

        // Reset state
        activeWindow = null;
        bufferedLogEvents.clear();
        snapshotChunkEvents = List.of();
    }

    /**
     * Cancels the given window without reconciliation.
     * <p>
     * Used when a snapshot chunk fails mid-window. Discards buffered events
     * and resets window state so a new window can be opened for retry.
     *
     * @param window the window to cancel
     */
    public void cancelWindow(WatermarkWindow window) {
        LOG.warn("Cancelling watermark window {} for table {} chunk {}",
                window.windowId(), window.tableId(), window.chunkIndex());
        bufferedLogEvents.clear();
        snapshotChunkEvents = List.of();
        activeWindow = null;
    }

    /**
     * Returns whether a reconciliation window is currently active.
     */
    public boolean isWindowActive() {
        return activeWindow != null;
    }

    /**
     * Returns the number of buffered log events in the current window.
     */
    public int bufferedLogEventCount() {
        return bufferedLogEvents.size();
    }
}
