package com.paulsnow.qcdcf.runtime.service;

import jakarta.enterprise.context.ApplicationScoped;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory tracker for snapshot and watermark window activity.
 * <p>
 * Maintains ring buffers for snapshot job history, watermark windows,
 * and watermark events detected in the WAL stream. All operations are
 * thread-safe via concurrent collections and atomic primitives.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class SnapshotMonitorService {

    /* ── Snapshot job tracking ───────────────────────────────────────── */

    private final Deque<SnapshotJobRecord> snapshotHistory = new ConcurrentLinkedDeque<>();
    private static final int MAX_HISTORY = 20;

    /* ── Watermark window tracking ───────────────────────────────────── */

    private final Map<UUID, WatermarkWindowRecord> activeWindows = new ConcurrentHashMap<>();
    private final Deque<WatermarkWindowRecord> completedWindows = new ConcurrentLinkedDeque<>();
    private static final int MAX_COMPLETED_WINDOWS = 50;

    /* ── Watermark events seen in WAL ────────────────────────────────── */

    private final AtomicLong watermarkEventsDetected = new AtomicLong();
    private final Deque<WatermarkEventRecord> recentWatermarkEvents = new ConcurrentLinkedDeque<>();
    private static final int MAX_RECENT_WATERMARK_EVENTS = 30;

    // ── Record types ────────────────────────────────────────────────────

    /**
     * A snapshot job record for the dashboard history.
     *
     * @param startedAt       when the snapshot was initiated
     * @param tableName       the table being snapshotted
     * @param status          current status (RUNNING, COMPLETE, FAILED)
     * @param chunksCompleted number of chunks completed so far
     * @param rowsRead        total rows read so far
     * @param completedAt     when the snapshot completed (null if still running)
     */
    public record SnapshotJobRecord(
            Instant startedAt,
            String tableName,
            String status,
            int chunksCompleted,
            long rowsRead,
            Instant completedAt
    ) {
        /** Returns a human-readable duration string. */
        public String duration() {
            Instant end = completedAt != null ? completedAt : Instant.now();
            Duration d = Duration.between(startedAt, end);
            return String.format("%02d:%02d:%02d", d.toHours(), d.toMinutesPart(), d.toSecondsPart());
        }
    }

    /**
     * A watermark window record for the dashboard.
     *
     * @param windowId               unique window identifier
     * @param tableName              the table being snapshotted
     * @param chunkIndex             the chunk index within the snapshot
     * @param lowMark                when the low watermark was written
     * @param highMark               when the high watermark was written (null if open)
     * @param closed                 whether the window has been closed
     * @param logEventsBuffered      number of log events buffered during the window
     * @param snapshotEventsReceived number of snapshot events received
     * @param logEventsDropped       number of log events dropped during reconciliation
     */
    public record WatermarkWindowRecord(
            UUID windowId,
            String tableName,
            int chunkIndex,
            Instant lowMark,
            Instant highMark,
            boolean closed,
            int logEventsBuffered,
            int snapshotEventsReceived,
            int logEventsDropped
    ) {
        /** Returns a truncated window ID for display. */
        public String shortId() {
            return windowId.toString().substring(0, 8);
        }

        /** Returns the window duration as a human-readable string. */
        public String duration() {
            if (highMark == null) {
                return "—";
            }
            Duration d = Duration.between(lowMark, highMark);
            long millis = d.toMillis();
            if (millis < 1000) {
                return millis + "ms";
            }
            return String.format("%.1fs", millis / 1000.0);
        }
    }

    /**
     * A watermark event detected in the WAL stream.
     *
     * @param detectedAt when the event was detected
     * @param boundary   the boundary type (LOW or HIGH)
     * @param windowId   the window identifier
     * @param tableName  the table name
     * @param chunkIndex the chunk index
     * @param lsn        the WAL log sequence number
     */
    public record WatermarkEventRecord(
            Instant detectedAt,
            String boundary,
            UUID windowId,
            String tableName,
            int chunkIndex,
            long lsn
    ) {
        /** Returns a truncated window ID for display. */
        public String shortId() {
            return windowId.toString().substring(0, 8);
        }

        /** Returns the LSN formatted as a hexadecimal string. */
        public String lsnHex() {
            return String.format("0/%08X", lsn);
        }
    }

    // ── Recording methods ───────────────────────────────────────────────

    /**
     * Record that a snapshot has started for the given table.
     *
     * @param tableName the table being snapshotted
     */
    public void recordSnapshotStarted(String tableName) {
        snapshotHistory.addFirst(new SnapshotJobRecord(
                Instant.now(), tableName, "RUNNING", 0, 0, null));
        trimDeque(snapshotHistory, MAX_HISTORY);
    }

    /**
     * Record that a snapshot chunk has been completed.
     *
     * @param tableName the table being snapshotted
     * @param chunk     the chunk index that was completed
     * @param rows      the cumulative row count
     */
    public void recordSnapshotChunkCompleted(String tableName, int chunk, long rows) {
        replaceLatestSnapshot(tableName, rec -> new SnapshotJobRecord(
                rec.startedAt(), tableName, "RUNNING", chunk, rows, null));
    }

    /**
     * Record that a snapshot has completed successfully.
     *
     * @param tableName the table that was snapshotted
     * @param totalRows the total number of rows read
     */
    public void recordSnapshotCompleted(String tableName, long totalRows) {
        replaceLatestSnapshot(tableName, rec -> new SnapshotJobRecord(
                rec.startedAt(), tableName, "COMPLETE", rec.chunksCompleted(), totalRows, Instant.now()));
    }

    /**
     * Record that a snapshot has failed.
     *
     * @param tableName the table whose snapshot failed
     * @param error     the error message
     */
    public void recordSnapshotFailed(String tableName, String error) {
        replaceLatestSnapshot(tableName, rec -> new SnapshotJobRecord(
                rec.startedAt(), tableName, "FAILED", rec.chunksCompleted(), rec.rowsRead(), Instant.now()));
    }

    /**
     * Record that a watermark window has been opened.
     *
     * @param windowId   the window identifier
     * @param tableName  the table name
     * @param chunkIndex the chunk index
     */
    public void recordWindowOpened(UUID windowId, String tableName, int chunkIndex) {
        activeWindows.put(windowId, new WatermarkWindowRecord(
                windowId, tableName, chunkIndex, Instant.now(), null, false, 0, 0, 0));
    }

    /**
     * Record that a watermark window has been closed after reconciliation.
     *
     * @param windowId       the window identifier
     * @param logBuffered    number of log events buffered
     * @param snapshotEvents number of snapshot events received
     * @param dropped        number of log events dropped
     */
    public void recordWindowClosed(UUID windowId, int logBuffered, int snapshotEvents, int dropped) {
        WatermarkWindowRecord open = activeWindows.remove(windowId);
        if (open != null) {
            WatermarkWindowRecord closed = new WatermarkWindowRecord(
                    windowId, open.tableName(), open.chunkIndex(),
                    open.lowMark(), Instant.now(), true,
                    logBuffered, snapshotEvents, dropped);
            completedWindows.addFirst(closed);
            trimDeque(completedWindows, MAX_COMPLETED_WINDOWS);
        }
    }

    /**
     * Record a watermark event detected in the WAL stream.
     *
     * @param boundary   the boundary type (LOW or HIGH)
     * @param windowId   the window identifier
     * @param tableName  the table name
     * @param chunkIndex the chunk index
     * @param lsn        the WAL log sequence number
     */
    public void recordWatermarkEventInWal(String boundary, UUID windowId, String tableName,
                                          int chunkIndex, long lsn) {
        watermarkEventsDetected.incrementAndGet();
        recentWatermarkEvents.addFirst(new WatermarkEventRecord(
                Instant.now(), boundary, windowId, tableName, chunkIndex, lsn));
        trimDeque(recentWatermarkEvents, MAX_RECENT_WATERMARK_EVENTS);
    }

    // ── Getters for dashboard ───────────────────────────────────────────

    /** Returns the snapshot job history, newest first. */
    public List<SnapshotJobRecord> snapshotHistory() {
        return List.copyOf(snapshotHistory);
    }

    /** Returns the currently active snapshot job, or {@code null} if none. */
    public SnapshotJobRecord activeSnapshot() {
        for (SnapshotJobRecord rec : snapshotHistory) {
            if ("RUNNING".equals(rec.status())) {
                return rec;
            }
        }
        return null;
    }

    /** Returns the currently active watermark windows. */
    public Collection<WatermarkWindowRecord> activeWindows() {
        return List.copyOf(activeWindows.values());
    }

    /** Returns recently completed watermark windows, newest first. */
    public List<WatermarkWindowRecord> completedWindows() {
        return List.copyOf(completedWindows);
    }

    /** Returns recent watermark events detected in the WAL, newest first. */
    public List<WatermarkEventRecord> recentWatermarkEvents() {
        return List.copyOf(recentWatermarkEvents);
    }

    /** Returns the total number of watermark events detected since startup. */
    public long totalWatermarkEventsDetected() {
        return watermarkEventsDetected.get();
    }

    // ── Private helpers ─────────────────────────────────────────────────

    private void replaceLatestSnapshot(String tableName,
                                       java.util.function.UnaryOperator<SnapshotJobRecord> updater) {
        // Find and replace the latest RUNNING record for this table
        var iterator = snapshotHistory.iterator();
        Deque<SnapshotJobRecord> temp = new ArrayDeque<>();
        boolean replaced = false;
        while (iterator.hasNext()) {
            SnapshotJobRecord rec = iterator.next();
            if (!replaced && rec.tableName().equals(tableName) && "RUNNING".equals(rec.status())) {
                temp.addLast(updater.apply(rec));
                replaced = true;
            } else {
                temp.addLast(rec);
            }
        }
        if (!replaced) {
            // No running record found — create one
            temp.addFirst(updater.apply(new SnapshotJobRecord(
                    Instant.now(), tableName, "RUNNING", 0, 0, null)));
        }
        snapshotHistory.clear();
        snapshotHistory.addAll(temp);
    }

    private static <T> void trimDeque(Deque<T> deque, int maxSize) {
        while (deque.size() > maxSize) {
            deque.pollLast();
        }
    }
}
