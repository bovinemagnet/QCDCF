package com.paulsnow.qcdcf.postgres.sql;

import com.paulsnow.qcdcf.core.snapshot.SnapshotChunkPlan;
import com.paulsnow.qcdcf.model.RowKey;
import com.paulsnow.qcdcf.model.TableId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostgresChunkSqlBuilder} identifier quoting and
 * keyset pagination SQL.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
class PostgresChunkSqlBuilderTest {

    private final PostgresChunkSqlBuilder builder = new PostgresChunkSqlBuilder();

    @Test
    void firstChunkHasNoWhereClause() {
        var plan = new SnapshotChunkPlan(new TableId("public", "customer"), 0, 100, null, null);

        String sql = builder.buildChunkQuery(plan, List.of("id"));

        assertThat(sql).isEqualTo(
                "SELECT * FROM \"public\".\"customer\" ORDER BY \"id\" LIMIT 100");
    }

    @Test
    void subsequentChunkUsesKeysetCondition() {
        var plan = new SnapshotChunkPlan(new TableId("public", "customer"), 1, 100,
                new RowKey(Map.of("id", 42)), null);

        String sql = builder.buildChunkQuery(plan, List.of("id"));

        assertThat(sql).isEqualTo(
                "SELECT * FROM \"public\".\"customer\" WHERE \"id\" > ? ORDER BY \"id\" LIMIT 100");
    }

    @Test
    void compositeKeyUsesRowValueComparison() {
        var plan = new SnapshotChunkPlan(new TableId("public", "orders"), 1, 50,
                new RowKey(Map.of("tenant_id", 1, "order_id", 2)), null);

        String sql = builder.buildChunkQuery(plan, List.of("tenant_id", "order_id"));

        assertThat(sql).isEqualTo(
                "SELECT * FROM \"public\".\"orders\" WHERE (\"tenant_id\", \"order_id\") > (?, ?) "
                        + "ORDER BY \"tenant_id\", \"order_id\" LIMIT 50");
    }

    @Test
    void mixedCaseAndReservedWordIdentifiersAreQuoted() {
        var plan = new SnapshotChunkPlan(new TableId("TestCDC", "MixedCase"), 1, 10,
                new RowKey(Map.of("Order", 7)), null);

        String sql = builder.buildChunkQuery(plan, List.of("Order"));

        assertThat(sql).isEqualTo(
                "SELECT * FROM \"TestCDC\".\"MixedCase\" WHERE \"Order\" > ? ORDER BY \"Order\" LIMIT 10");
    }

    @Test
    void embeddedQuotesInIdentifiersAreEscaped() {
        var plan = new SnapshotChunkPlan(new TableId("public", "we\"ird"), 0, 10, null, null);

        String sql = builder.buildChunkQuery(plan, List.of("id"));

        assertThat(sql).contains("\"we\"\"ird\"");
    }
}
