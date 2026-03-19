package com.paulsnow.qcdcf.testkit.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Loads SQL scripts into a test database.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public final class TestSchemaLoader {

    private TestSchemaLoader() {}

    /**
     * Executes a SQL script from the classpath.
     *
     * @param connection the database connection
     * @param resourcePath the classpath path to the SQL script
     * @throws SQLException if the SQL execution fails
     */
    public static void loadSchema(Connection connection, String resourcePath) throws SQLException {
        String sql = readResource(resourcePath);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static String readResource(String path) {
        try (InputStream is = TestSchemaLoader.class.getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + path);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read resource: " + path, e);
        }
    }
}
