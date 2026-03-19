package com.paulsnow.qcdcf.testkit.fixtures;

import com.paulsnow.qcdcf.model.TableId;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates and populates a standard {@code customer} test table.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public final class CustomerTableFixture {

    public static final TableId TABLE_ID = new TableId("public", "customer");

    private CustomerTableFixture() {}

    /**
     * Creates the customer table and inserts sample rows.
     *
     * @param connection the database connection
     * @throws SQLException if table creation or data insertion fails
     */
    public static void create(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS customer (
                        id          SERIAL PRIMARY KEY,
                        name        VARCHAR(255) NOT NULL,
                        email       VARCHAR(255),
                        created_at  TIMESTAMP DEFAULT NOW()
                    )
                    """);
            stmt.execute("""
                    INSERT INTO customer (name, email) VALUES
                        ('Alice', 'alice@example.com'),
                        ('Bob', 'bob@example.com'),
                        ('Charlie', 'charlie@example.com')
                    ON CONFLICT DO NOTHING
                    """);
        }
    }
}
