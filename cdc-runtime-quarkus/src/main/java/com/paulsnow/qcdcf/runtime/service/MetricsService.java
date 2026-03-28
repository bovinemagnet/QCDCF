package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.model.ChangeEnvelope;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.scheduler.Scheduled;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory metrics tracker for the CDC pipeline.
 * <p>
 * Maintains counters for processed events by operation type, a ring buffer of
 * recent events for the dashboard, and an error log. All operations are
 * thread-safe via atomic primitives and concurrent collections.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class MetricsService {

    private final AtomicLong totalEventsProcessed = new AtomicLong();
    private final AtomicLong totalInserts = new AtomicLong();
    private final AtomicLong totalUpdates = new AtomicLong();
    private final AtomicLong totalDeletes = new AtomicLong();
    private final AtomicLong totalSnapshotReads = new AtomicLong();
    private final AtomicLong lastEventTimestamp = new AtomicLong();
    private final AtomicLong lastLsn = new AtomicLong();
    private final AtomicLong errorsCount = new AtomicLong();

    private final AtomicLong walReconnectAttempts = new AtomicLong();
    private final AtomicLong sinkPublishRetries = new AtomicLong();
    private final AtomicLong snapshotChunkRetries = new AtomicLong();

    private final ConcurrentLinkedDeque<RecentEvent> recentEvents = new ConcurrentLinkedDeque<>();
    private static final int MAX_RECENT_EVENTS = 20;

    private final ConcurrentLinkedDeque<ErrorEntry> recentErrors = new ConcurrentLinkedDeque<>();
    private static final int MAX_RECENT_ERRORS = 10;

    private final Instant startedAt = Instant.now();

    /**
     * A recent event entry for the dashboard display.
     *
     * @param timestamp when the event was recorded
     * @param table     the source table name
     * @param operation the operation type
     * @param key       the row key as a string
     */
    public record RecentEvent(Instant timestamp, String table, String operation, String key) {}

    /**
     * An error entry for the dashboard error log.
     *
     * @param timestamp when the error occurred
     * @param message   the error message
     */
    public record ErrorEntry(Instant timestamp, String message) {}

    /**
     * Record a successfully processed change event, incrementing the appropriate counters
     * and adding to the recent events ring buffer.
     *
     * @param event the change envelope that was processed
     */
    public void recordEvent(ChangeEnvelope event) {
        totalEventsProcessed.incrementAndGet();
        lastEventTimestamp.set(event.captureTimestamp().toEpochMilli());

        if (event.position() != null) {
            lastLsn.set(event.position().lsn());
        }

        switch (event.operation()) {
            case INSERT -> totalInserts.incrementAndGet();
            case UPDATE -> totalUpdates.incrementAndGet();
            case DELETE -> totalDeletes.incrementAndGet();
            case SNAPSHOT_READ -> totalSnapshotReads.incrementAndGet();
            default -> { /* HEARTBEAT and others — counted in total only */ }
        }

        var recentEvent = new RecentEvent(
                Instant.now(),
                event.tableId().canonicalName(),
                event.operation().name(),
                event.key().toCanonical()
        );
        recentEvents.addFirst(recentEvent);
        while (recentEvents.size() > MAX_RECENT_EVENTS) {
            recentEvents.pollLast();
        }
    }

    /**
     * Record an error message, adding to the error ring buffer.
     *
     * @param message the error description
     */
    public void recordError(String message) {
        errorsCount.incrementAndGet();
        recentErrors.addFirst(new ErrorEntry(Instant.now(), message));
        while (recentErrors.size() > MAX_RECENT_ERRORS) {
            recentErrors.pollLast();
        }
    }

    /** Total number of events processed since startup. */
    public long totalEvents() {
        return totalEventsProcessed.get();
    }

    /** Total INSERT operations. */
    public long totalInserts() {
        return totalInserts.get();
    }

    /** Total UPDATE operations. */
    public long totalUpdates() {
        return totalUpdates.get();
    }

    /** Total DELETE operations. */
    public long totalDeletes() {
        return totalDeletes.get();
    }

    /** Total SNAPSHOT_READ operations. */
    public long totalSnapshotReads() {
        return totalSnapshotReads.get();
    }

    /** Total error count. */
    public long errorsCount() {
        return errorsCount.get();
    }

    /** Record a WAL reconnection attempt. */
    public void recordWalReconnectAttempt() { walReconnectAttempts.incrementAndGet(); }

    /** Record a sink publish retry. */
    public void recordSinkPublishRetry() { sinkPublishRetries.incrementAndGet(); }

    /** Record a snapshot chunk retry. */
    public void recordSnapshotChunkRetry() { snapshotChunkRetries.incrementAndGet(); }

    /** Total WAL reconnection attempts. */
    public long walReconnectAttempts() { return walReconnectAttempts.get(); }

    /** Total sink publish retries. */
    public long sinkPublishRetries() { return sinkPublishRetries.get(); }

    /** Total snapshot chunk retries. */
    public long snapshotChunkRetries() { return snapshotChunkRetries.get(); }

    /** Last known LSN value, or 0 if no events received. */
    public long lastLsn() {
        return lastLsn.get();
    }

    /** Last LSN formatted as a hexadecimal string. */
    public String lastLsnHex() {
        long lsn = lastLsn.get();
        if (lsn == 0) {
            return "N/A";
        }
        return String.format("0/%08X", lsn);
    }

    /**
     * Calculate an approximate events-per-second rate based on total events
     * and uptime.
     *
     * @return the average events per second since startup
     */
    public double eventsPerSecond() {
        long uptimeSeconds = Duration.between(startedAt, Instant.now()).toSeconds();
        if (uptimeSeconds == 0) {
            return 0.0;
        }
        return (double) totalEventsProcessed.get() / uptimeSeconds;
    }

    /** Formatted events-per-second string with two decimal places. */
    public String eventsPerSecondFormatted() {
        return String.format("%.2f", eventsPerSecond());
    }

    /** Returns the most recent events (up to 20), newest first. */
    public List<RecentEvent> recentEvents() {
        return List.copyOf(recentEvents);
    }

    /** Returns the most recent errors (up to 10), newest first. */
    public List<ErrorEntry> recentErrors() {
        return List.copyOf(recentErrors);
    }

    /**
     * Calculate the percentage of a given count relative to total events.
     * Returns 0 if no events have been processed.
     *
     * @param count the count to calculate the percentage for
     * @return the percentage as an integer (0-100)
     */
    public int percentage(long count) {
        long total = totalEventsProcessed.get();
        if (total == 0) {
            return 0;
        }
        return (int) Math.round((double) count / total * 100);
    }

    // ── Historical metrics (sparkline data) ──────────────────────────────

    private final ConcurrentLinkedDeque<MetricSnapshot> history = new ConcurrentLinkedDeque<>();
    private static final int HISTORY_SIZE = 60;

    /**
     * A point-in-time snapshot of all counters, used for sparkline computation.
     *
     * @param timestamp      when the snapshot was taken
     * @param totalEvents    total events at that instant
     * @param inserts        total inserts at that instant
     * @param updates        total updates at that instant
     * @param deletes        total deletes at that instant
     * @param snapshotReads  total snapshot reads at that instant
     * @param errors         total errors at that instant
     */
    public record MetricSnapshot(
            Instant timestamp,
            long totalEvents,
            long inserts,
            long updates,
            long deletes,
            long snapshotReads,
            long errors
    ) {}

    /**
     * Scheduled task that captures a snapshot of current counters every second.
     */
    @Scheduled(every = "1s")
    void captureMetricSnapshot() {
        captureSnapshot();
    }

    /**
     * Capture a snapshot of the current metric counters and add it to the
     * history ring buffer.
     */
    public void captureSnapshot() {
        history.addLast(new MetricSnapshot(
                Instant.now(),
                totalEventsProcessed.get(),
                totalInserts.get(),
                totalUpdates.get(),
                totalDeletes.get(),
                totalSnapshotReads.get(),
                errorsCount.get()
        ));
        while (history.size() > HISTORY_SIZE) {
            history.removeFirst();
        }
    }

    /**
     * Returns per-second event rates computed from consecutive snapshots.
     *
     * @return list of events-per-second values (one fewer than snapshot count)
     */
    public List<Double> eventsPerSecondHistory() {
        return rateHistory(MetricSnapshot::totalEvents);
    }

    /** Per-second INSERT rate history. */
    public List<Double> insertRateHistory() {
        return rateHistory(MetricSnapshot::inserts);
    }

    /** Per-second UPDATE rate history. */
    public List<Double> updateRateHistory() {
        return rateHistory(MetricSnapshot::updates);
    }

    /** Per-second DELETE rate history. */
    public List<Double> deleteRateHistory() {
        return rateHistory(MetricSnapshot::deletes);
    }

    /** Per-second error rate history. */
    public List<Double> errorRateHistory() {
        return rateHistory(MetricSnapshot::errors);
    }

    /** Returns the raw history snapshots (defensive copy). */
    public List<MetricSnapshot> historySnapshots() {
        return List.copyOf(history);
    }

    private List<Double> rateHistory(java.util.function.ToLongFunction<MetricSnapshot> extractor) {
        List<MetricSnapshot> snapshots = new ArrayList<>(history);
        if (snapshots.size() < 2) {
            return List.of(0.0);
        }
        List<Double> rates = new ArrayList<>();
        for (int i = 1; i < snapshots.size(); i++) {
            long diff = extractor.applyAsLong(snapshots.get(i)) - extractor.applyAsLong(snapshots.get(i - 1));
            rates.add(Math.max(0, (double) diff));
        }
        return rates;
    }
}
