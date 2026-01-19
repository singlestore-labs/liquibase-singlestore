package liquibase.ext.singlestore;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.ResourceBundle;
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
    public void setConnection(DatabaseConnection conn) {
        // Modify the URL in the connection before passing to parent
        modifyConnectionUrl(conn);
        super.setConnection(conn);
    }

    private void modifyConnectionUrl(DatabaseConnection conn) {
        if (!(conn instanceof JdbcConnection)) {
            return;
        }
        String originalUrl = conn.getURL();
        if (originalUrl == null || !originalUrl.startsWith("jdbc:singlestore")) {
            return;
        }
        String modifiedUrl = addConnectionAttributes(originalUrl);        
        try {
            Field urlField = conn.getClass().getDeclaredField("url");
            urlField.setAccessible(true);
            urlField.set(conn, modifiedUrl);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // Silently fail if we can't modify the URL
        }
    }

    private String addConnectionAttributes(String originalUrl) {
        String liquibaseVersion = getLiquibaseVersion();
        String connectionAttributes = "_connector_name:Liquibase,_connector_version:" + liquibaseVersion;
        
        String separator = originalUrl.contains("?") ? "&" : "?";
        
        if (originalUrl.contains("connectionAttributes=")) {
            // Append to existing connectionAttributes
            return originalUrl.replaceFirst(
                "(connectionAttributes=[^&]*)",
                "$1," + connectionAttributes
            );
        } else {
            // Add new connectionAttributes parameter
            return originalUrl + separator + "connectionAttributes=" + connectionAttributes;
        }
    }

    private String getLiquibaseVersion() {
        try {
            ResourceBundle bundle = ResourceBundle.getBundle("liquibase/build");
            return bundle.getString("build.version");
        } catch (Exception e) {
            return "unknown";
        }
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