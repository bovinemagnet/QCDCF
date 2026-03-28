package com.paulsnow.qcdcf.postgres.metadata;

/**
 * Metadata for a single database column.
 *
 * @param name         the column name
 * @param dataType     the PostgreSQL data type name
 * @param ordinalPosition the column's position in the table (1-based)
 * @param nullable     whether the column allows NULL values
 * @param primaryKey   whether the column is part of the primary key
 * @author Paul Snow
 * @since 0.0.0
 */
public record ColumnMetadata(
        String name,
        String dataType,
        int ordinalPosition,
        boolean nullable,
        boolean primaryKey
) {}
