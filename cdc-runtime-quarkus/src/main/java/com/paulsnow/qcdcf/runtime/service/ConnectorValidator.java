package com.paulsnow.qcdcf.runtime.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.sql.*;
import java.util.*;

/**
 * Validates PostgreSQL prerequisites for CDC operation.
 * <p>
 * Uses parameterised queries to prevent SQL injection.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
@ApplicationScoped
public class ConnectorValidator {

    private static final Logger LOG = Logger.getLogger(ConnectorValidator.class);

    public List<Map<String, String>> validateWalLevel(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
            rs.next();
            String walLevel = rs.getString(1);
            if ("logical".equals(walLevel)) {
                return List.of(Map.of("check", "WAL level", "status", "OK", "value", walLevel));
            }
            return List.of(Map.of("check", "WAL level", "status", "FAIL", "value", walLevel, "expected", "logical"));
        }
    }

    public List<Map<String, String>> validateSlot(Connection conn, String slotName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT active FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return List.of(Map.of("check", "Replication slot '" + slotName + "'", "status", "OK",
                            "active", String.valueOf(rs.getBoolean("active"))));
                }
                return List.of(Map.of("check", "Replication slot '" + slotName + "'", "status", "WARN",
                        "detail", "Not found — will be created on start"));
            }
        }
    }

    public List<Map<String, String>> validatePublication(Connection conn, String pubName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM pg_publication WHERE pubname = ?")) {
            ps.setString(1, pubName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return List.of(Map.of("check", "Publication '" + pubName + "'", "status", "OK"));
                }
                return List.of(Map.of("check", "Publication '" + pubName + "'", "status", "FAIL",
                        "detail", "Not found — create with: CREATE PUBLICATION " + pubName + " FOR TABLE ..."));
            }
        }
    }

    public List<Map<String, String>> validateWatermarkTable(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?")) {
            ps.setString(1, "qcdcf_watermark");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return List.of(Map.of("check", "Watermark table", "status", "OK"));
                }
                return List.of(Map.of("check", "Watermark table", "status", "WARN",
                        "detail", "Not found — will be created on first snapshot"));
            }
        }
    }

    public boolean validateAll(Connection conn, String slotName, String pubName) {
        try {
            var results = new ArrayList<Map<String, String>>();
            results.addAll(validateWalLevel(conn));
            results.addAll(validateSlot(conn, slotName));
            results.addAll(validatePublication(conn, pubName));
            results.addAll(validateWatermarkTable(conn));
            boolean hasFail = results.stream().anyMatch(r -> "FAIL".equals(r.get("status")));
            if (hasFail) {
                results.stream().filter(r -> "FAIL".equals(r.get("status")))
                        .forEach(r -> LOG.errorf("Validation FAIL: %s — %s", r.get("check"), r.getOrDefault("detail", r.getOrDefault("value", ""))));
            }
            return !hasFail;
        } catch (SQLException e) {
            LOG.errorf("Validation failed: %s", e.getMessage());
            return false;
        }
    }
}
