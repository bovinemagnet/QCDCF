package com.paulsnow.qcdcf.runtime.service;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ConnectorValidatorTest {

    @Test
    void detectsNonLogicalWalLevel() throws Exception {
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SHOW wal_level")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn("replica");

        var validator = new ConnectorValidator();
        List<Map<String, String>> results = validator.validateWalLevel(conn);

        assertThat(results).hasSize(1);
        assertThat(results.getFirst().get("status")).isEqualTo("FAIL");
    }

    @Test
    void passesLogicalWalLevel() throws Exception {
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SHOW wal_level")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn("logical");

        var validator = new ConnectorValidator();
        List<Map<String, String>> results = validator.validateWalLevel(conn);

        assertThat(results).hasSize(1);
        assertThat(results.getFirst().get("status")).isEqualTo("OK");
    }
}
