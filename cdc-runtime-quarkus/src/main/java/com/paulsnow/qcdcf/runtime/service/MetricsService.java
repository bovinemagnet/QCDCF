package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.model.ChangeEnvelope;
import jakarta.enterprise.context.ApplicationScoped;

import java.time.Duration;
import java.time.Instant;
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
}
