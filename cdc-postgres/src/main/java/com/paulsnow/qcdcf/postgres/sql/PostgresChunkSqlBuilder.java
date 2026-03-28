package com.paulsnow.qcdcf.postgres.sql;

import com.paulsnow.qcdcf.core.snapshot.SnapshotChunkPlan;
import com.paulsnow.qcdcf.model.RowKey;

import java.util.List;

/**
 * Builds keyset-paginated SQL queries for snapshot chunk reads.
 * <p>
 * Uses keyset pagination (never OFFSET) for consistent, non-blocking reads
 * under READ COMMITTED isolation.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public class PostgresChunkSqlBuilder {

    /**
     * Builds a SELECT query for the given chunk plan.
     *
     * @param plan           the chunk plan
     * @param primaryKeyColumns the primary key columns for keyset pagination
     * @return the SQL query string
     */
    public String buildChunkQuery(SnapshotChunkPlan plan, List<String> primaryKeyColumns) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ")
                .append(plan.tableId().canonicalName());

        if (plan.lowerBound() != null && !primaryKeyColumns.isEmpty()) {
            sql.append(" WHERE ");
            appendKeysetCondition(sql, primaryKeyColumns, plan.lowerBound());
        }

        sql.append(" ORDER BY ");
        sql.append(String.join(", ", primaryKeyColumns));
        sql.append(" LIMIT ").append(plan.chunkSize());

        return sql.toString();
    }

    private void appendKeysetCondition(StringBuilder sql, List<String> columns, RowKey lowerBound) {
        if (columns.size() == 1) {
            String col = columns.getFirst();
            sql.append(col).append(" > ?");
        } else {
            // Composite key: row-value comparison
            sql.append("(");
            sql.append(String.join(", ", columns));
            sql.append(") > (");
            sql.append("?, ".repeat(columns.size() - 1)).append("?");
            sql.append(")");
        }
    }
}
