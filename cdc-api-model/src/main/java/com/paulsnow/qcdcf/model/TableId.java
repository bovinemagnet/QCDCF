package com.paulsnow.qcdcf.model;

import java.util.Objects;

/**
 * Identifies a database table by schema and name.
 *
 * @param schema the database schema (e.g. "public")
 * @param table  the table name
 * @author Paul Snow
 * @since 0.0.0
 */
public record TableId(String schema, String table) {

    public TableId {
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(table, "table must not be null");
    }

    /**
     * Returns the fully qualified canonical name: {@code schema.table}.
     */
    public String canonicalName() {
        return schema + "." + table;
    }

    @Override
    public String toString() {
        return canonicalName();
    }
}
