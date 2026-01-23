package liquibase.ext.singlestore;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import liquibase.Scope;
import liquibase.database.DatabaseConnection;
import liquibase.database.core.MariaDBDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;

public class SingleStoreDatabase extends MariaDBDatabase {

    public static final String PRODUCT_NAME = "SingleStore";

    public SingleStoreDatabase() {
        super.sequenceNextValueFunction = null;
        super.unmodifiableDataTypes = new ArrayList<>();
    }

    @Override
    public String getShortName() {
        return "singlestore";
    }

    @Override
    public String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        if (conn.getDatabaseProductName().equals("MySQL")) {
            try (Statement stmt = ((JdbcConnection) conn).createStatement();
                 ResultSet rs = stmt.executeQuery("select @@memsql_version")) {
                if (rs.next()) {
                    if (!rs.getString(1).isEmpty()) {
                        return true;
                    }
                }
            } catch (SQLException e) {
                return false;
            }
        }
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:singlestore")) {
            return "com.singlestore.jdbc.Driver";
        }
        return super.getDefaultDriver(url);
    }

    @Override
    public void setConnection(final DatabaseConnection conn) {
        DatabaseConnection connectionToUse = conn;
        
        // If a user creates a JDBC connection manually and passes it to Liquibase,
        // this method will be called. Normally, a connection will be opened by
        // Liquibase based on the connection URL configured. In that case, the
        // SingleStoreConnection class will ensure connectionAttributes are set.
        // However, when a user creates the connection programmatically and passes it to
        // Liquibase, we need to check if connectionAttributes are missing and create
        // a replacement connection with them.
        if (!(conn instanceof SingleStoreConnection)
            && conn instanceof JdbcConnection) {
            JdbcConnection jdbcConn = (JdbcConnection) conn;
            String url = jdbcConn.getURL();
            
            // Check if it's a SingleStore connection without connectionAttributes
            if (url != null && url.startsWith("jdbc:singlestore") && !url.contains("connectionAttributes=")) {
                // Create a replacement connection with connectionAttributes
                try {
                    String modifiedUrl = SingleStoreConnection.addConnectionAttributesToUrl(url);
                    connectionToUse = new SingleStoreConnection(
                        DriverManager.getConnection(modifiedUrl),
                        conn  // Pass original connection so it gets closed when replacement is closed
                    );
                    
                    Scope.getCurrentScope().getLog(getClass()).info(
                        "Replaced manually-created connection with one that includes SingleStore connection attributes");
                } catch (SQLException e) {
                    // If we can't create a replacement connection, use the original
                    Scope.getCurrentScope().getLog(getClass()).fine(
                        "Could not add connection attributes to manually-created connection: " + e.getMessage());
                }
            }
        }
        
        super.setConnection(connectionToUse);
    }

    @Override
    protected String getMinimumVersionForFractionalDigitsForTimestamp() {
      return "6.8.0";
    }

    @Override
    public boolean supportsSequences() {
      return false;
    }
}