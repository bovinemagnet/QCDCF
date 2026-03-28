package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.runtime.config.ConnectorRuntimeConfig;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Queries PostgreSQL for replication slot health and WAL lag metrics.
 * <p>
 * Maintains a rolling history of WAL lag samples for sparkline rendering
 * and provides structured health data for dashboard display. This service
 * is the primary source for the Replication Health dashboard — a growing
 * slot WAL lag is the number one production incident with logical replication.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class ReplicationHealthService {

    @Inject
    DataSource dataSource;

    @Inject
    ConnectorRuntimeConfig config;

    /** Historical WAL lag samples for sparkline (last 60 samples). */
    private final Deque<WalLagSample> lagHistory = new ConcurrentLinkedDeque<>();
    private static final int MAX_HISTORY = 60;

    // ── Records ─────────────────────────────────────────────────────────

    /**
     * A point-in-time WAL lag sample.
     *
     * @param timestamp when the sample was taken
     * @param lagBytes  WAL lag in bytes at that instant
     */
    public record WalLagSample(Instant timestamp, long lagBytes) {}

    /**
     * Health status of a replication slot.
     *
     * @param slotName            the slot name
     * @param plugin              the output plugin (e.g. pgoutput)
     * @param slotType            the slot type (logical or physical)
     * @param active              whether the slot is currently active
     * @param confirmedFlushLsn   confirmed flush LSN as raw value
     * @param confirmedFlushLsnHex confirmed flush LSN in hex format
     * @param restartLsn          restart LSN as raw value
     * @param restartLsnHex       restart LSN in hex format
     * @param walLagBytes         WAL lag in bytes
     * @param walLagFormatted     WAL lag as human-readable string
     * @param catalogXmin         the catalog xmin, or null
     */
    public record SlotHealth(
            String slotName, String plugin, String slotType,
            boolean active, long confirmedFlushLsn, String confirmedFlushLsnHex,
            long restartLsn, String restartLsnHex,
            long walLagBytes, String walLagFormatted,
            String catalogXmin
    ) {}

    /**
     * General replication statistics from PostgreSQL.
     *
     * @param walLevel              the wal_level setting
     * @param currentWalLsn         current WAL LSN as raw value
     * @param currentWalLsnHex      current WAL LSN in hex format
     * @param maxReplicationSlots   max_replication_slots setting
     * @param usedReplicationSlots  number of slots currently in use
     * @param maxWalSenders         max_wal_senders setting
     * @param activeWalSenders      number of active WAL senders
     * @param walSegmentSize        WAL segment size
     */
    public record ReplicationStats(
            String walLevel, long currentWalLsn, String currentWalLsnHex,
            int maxReplicationSlots, int usedReplicationSlots,
            int maxWalSenders, int activeWalSenders,
            String walSegmentSize
    ) {}

    // ── Public API ──────────────────────────────────────────────────────

    /**
     * Queries slot health from {@code pg_replication_slots}.
     *
     * @return the slot health, or {@code null} if the slot does not exist or the database is unreachable
     */
    public SlotHealth getSlotHealth() {
        String sql = """
                SELECT slot_name, plugin, slot_type, active,
                       confirmed_flush_lsn::text, restart_lsn::text,
                       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS wal_lag_bytes,
                       catalog_xmin::text
                FROM pg_replication_slots
                WHERE slot_name = ?
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, config.source().slotName());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long walLag = rs.getLong("wal_lag_bytes");
                    return new SlotHealth(
                            rs.getString("slot_name"),
                            rs.getString("plugin"),
                            rs.getString("slot_type"),
                            rs.getBoolean("active"),
                            0L,
                            defaultIfNull(rs.getString("confirmed_flush_lsn"), "N/A"),
                            0L,
                            defaultIfNull(rs.getString("restart_lsn"), "N/A"),
                            walLag,
                            formatBytes(walLag),
                            defaultIfNull(rs.getString("catalog_xmin"), "N/A")
                    );
                }
            }
        } catch (Exception e) {
            // Database unavailable — return null
        }
        return null;
    }

    /**
     * Queries general replication statistics from PostgreSQL.
     *
     * @return the replication stats, or a default "unavailable" instance if the database is unreachable
     */
    public ReplicationStats getReplicationStats() {
        try (Connection conn = dataSource.getConnection()) {
            String walLevel = showSetting(conn, "wal_level");
            int maxSlots = parseIntSetting(showSetting(conn, "max_replication_slots"));
            int maxSenders = parseIntSetting(showSetting(conn, "max_wal_senders"));
            String walSegmentSize = showSetting(conn, "wal_segment_size");

            String currentLsnHex = "N/A";
            long currentLsn = 0;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT pg_current_wal_lsn()::text")) {
                if (rs.next()) {
                    currentLsnHex = rs.getString(1);
                }
            }

            int usedSlots = 0;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*)::int FROM pg_replication_slots")) {
                if (rs.next()) {
                    usedSlots = rs.getInt(1);
                }
            }

            int activeSenders = 0;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*)::int FROM pg_stat_replication")) {
                if (rs.next()) {
                    activeSenders = rs.getInt(1);
                }
            }

            return new ReplicationStats(
                    walLevel, currentLsn, currentLsnHex,
                    maxSlots, usedSlots,
                    maxSenders, activeSenders,
                    walSegmentSize
            );
        } catch (Exception e) {
            return new ReplicationStats("unavailable", 0, "N/A", 0, 0, 0, 0, "N/A");
        }
    }

    /**
     * Returns WAL lag history values for sparkline rendering.
     *
     * @return list of lag values in bytes as doubles
     */
    public List<Double> walLagHistory() {
        return lagHistory.stream().map(s -> (double) s.lagBytes()).toList();
    }

    /**
     * Returns whether WAL lag is considered high (over 100 MB).
     *
     * @param lagBytes the lag in bytes
     * @return {@code true} if lag exceeds 100 MB
     */
    public static boolean isHighLag(long lagBytes) {
        return lagBytes > 100L * 1024 * 1024;
    }

    /**
     * Returns whether WAL lag is considered moderate (over 1 MB).
     *
     * @param lagBytes the lag in bytes
     * @return {@code true} if lag exceeds 1 MB
     */
    public static boolean isModerateLag(long lagBytes) {
        return lagBytes > 1024L * 1024;
    }

    /**
     * Returns the CSS colour class for a given WAL lag value.
     *
     * @param lagBytes the lag in bytes
     * @return a Tailwind CSS text colour class
     */
    public static String lagColour(long lagBytes) {
        if (isHighLag(lagBytes)) {
            return "text-red-400";
        } else if (isModerateLag(lagBytes)) {
            return "text-yellow-400";
        }
        return "text-emerald-400";
    }

    // ── Scheduled sampling ──────────────────────────────────────────────

    /**
     * Samples WAL lag every 5 seconds and appends to the history buffer.
     */
    @Scheduled(every = "5s")
    void sampleWalLag() {
        try {
            SlotHealth health = getSlotHealth();
            if (health != null) {
                lagHistory.addLast(new WalLagSample(Instant.now(), health.walLagBytes()));
                while (lagHistory.size() > MAX_HISTORY) {
                    lagHistory.removeFirst();
                }
            }
        } catch (Exception e) {
            // Database may not be available — skip sample
        }
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /**
     * Formats a byte count as a human-readable string.
     *
     * @param bytes the byte count
     * @return formatted string (e.g. "1.2 MB")
     */
    static String formatBytes(long bytes) {
        if (bytes < 0) {
            return "N/A";
        }
        if (bytes < 1024L) {
            return bytes + " B";
        }
        if (bytes < 1024L * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static String showSetting(Connection conn, String setting) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW " + setting)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (Exception ignored) {
            // setting may not exist
        }
        return "N/A";
    }

    private static int parseIntSetting(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String defaultIfNull(String value, String fallback) {
        return value != null ? value : fallback;
    }
}
