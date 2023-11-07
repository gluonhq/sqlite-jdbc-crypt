package org.sqlite.mc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import org.junit.jupiter.api.Test;

public class SQLiteMCSQLInterfaceTest {

    private static final String SQL_TABLE =
            "CREATE TABLE IF NOT EXISTS warehouses ("
                    + "	id integer PRIMARY KEY,"
                    + "	name text NOT NULL,"
                    + "	capacity real"
                    + ");";

    public String createFile() throws IOException {
        File tmpFile = File.createTempFile("tmp-sqlite", ".db");
        tmpFile.deleteOnExit();
        return tmpFile.getAbsolutePath();
    }

    public boolean databaseIsReadable(Connection connection) {
        if (connection == null) return false;
        try {
            Statement st = connection.createStatement();
            ResultSet resultSet = st.executeQuery("SELECT count(*) as nb FROM sqlite_master");
            resultSet.next();
            // System.out.println("The out is : " + resultSet.getString("nb"));
            assertThat(resultSet.getString("nb")).isEqualTo("1");
            //                    "1",
            //                    ,
            //                    "When reading the database, the result should contain the number
            // 1");
            return true;
        } catch (SQLException e) {
            // System.out.println(e.getMessage());
            return false;
        }
    }

    public void applySchema(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute(SQL_TABLE);
    }

    public void plainDatabaseCreate(String dbPath) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:sqlite:file:" + dbPath);
        applySchema(conn);
        conn.close();
    }

    public Connection plainDatabaseOpen(String dbPath) throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:file:" + dbPath);
    }

    @Test
    public void plainDatabaseTest() throws IOException, SQLException {
        String path = createFile();
        // 1.  Open + Write
        plainDatabaseCreate(path);

        // 2. Ensure another Connection can read the databse written
        Connection c = plainDatabaseOpen(path);
        assertThat(databaseIsReadable(c))
                .isTrue(); // , "The plain database should be always readable");
        c.close();
    }
}
