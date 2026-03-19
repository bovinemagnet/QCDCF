package com.paulsnow.qcdcf.core.watermark;

import com.paulsnow.qcdcf.model.TableId;

/**
 * Manages the lifecycle of watermark windows.
 * <p>
 * A watermark window brackets a snapshot chunk read:
 * <ol>
 *   <li>Open window: write low watermark</li>
 *   <li>Read snapshot chunk</li>
 *   <li>Close window: write high watermark</li>
 *   <li>Reconcile: remove key collisions between log and snapshot events</li>
 * </ol>
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public interface WatermarkCoordinator {

    /**
     * Opens a new watermark window for the given table and chunk.
     *
     * @param tableId    the table being snapshotted
     * @param chunkIndex the chunk index within the snapshot
     * @return the opened window
     */
    WatermarkWindow openWindow(TableId tableId, int chunkIndex);

    /**
     * Closes the given window by writing the high watermark.
     *
     * @param window the window to close
     * @return the closed window with the high watermark set
     */
    WatermarkWindow closeWindow(WatermarkWindow window);
}
