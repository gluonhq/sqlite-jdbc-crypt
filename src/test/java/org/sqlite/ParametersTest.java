package org.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.sql.*;
import org.junit.jupiter.api.Test;

public class ParametersTest {

    @Test
    public void testSqliteConfigViaURI() throws Throwable {
        File testDB = File.createTempFile("test.db", "", new File("target"));
        testDB.deleteOnExit();

        String uri =
                "jdbc:sqlite:file:"
                        + testDB
                        + "?cache=private&busy_timeout=1800000&auto_vacuum=2&journal_mode=truncate&synchronous=full&cache_size=-65536";
        try (Connection connection = DriverManager.getConnection(uri)) {
            try (Statement stat = connection.createStatement()) {
                stat.execute("select 1 from sqlite_master");

                checkPragma(stat, "busy_timeout", "1800000");
                checkPragma(stat, "auto_vacuum", "2");
                checkPragma(stat, "journal_mode", "truncate");
                checkPragma(stat, "synchronous", "2");
                checkPragma(stat, "cache_size", "-65536");
                assertThat(
                                ((SQLiteConnection) stat.getConnection())
                                        .getDatabase()
                                        .getConfig()
                                        .isEnabledSharedCache())
                        .isFalse();
                assertThat(
                                ((SQLiteConnection) stat.getConnection())
                                        .getDatabase()
                                        .getConfig()
                                        .isEnabledSharedCacheConnection())
                        .isFalse();
            }
        }
    }

    private void checkPragma(Statement stat, String key, String expectedValue) throws SQLException {
        try (ResultSet resultSet = stat.executeQuery("pragma " + key + ";")) {
            resultSet.next();
            String value = resultSet.getString(1);
            assertThat(value).isEqualTo(expectedValue);
        }
    }
}
