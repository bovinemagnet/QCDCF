package com.paulsnow.qcdcf.model;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Represents the primary key of a captured row as an ordered map of column names to values.
 *
 * @param columns the primary key columns and their values
 * @author Paul Snow
 * @since 0.0.0
 */
public record RowKey(Map<String, Object> columns) {

    public RowKey {
        Objects.requireNonNull(columns, "columns must not be null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }
        // Defensive copy into sorted map for deterministic ordering
        columns = Map.copyOf(new TreeMap<>(columns));
    }

    /**
     * Returns a deterministic canonical string representation suitable for hashing and comparison.
     * Keys are sorted alphabetically; values use their {@code toString()} representation.
     */
    public String toCanonical() {
        return new TreeMap<>(columns).entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return "RowKey[" + toCanonical() + "]";
    }
}
