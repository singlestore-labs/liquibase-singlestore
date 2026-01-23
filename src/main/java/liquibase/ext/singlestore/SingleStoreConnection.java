package liquibase.ext.singlestore;

import java.sql.Connection;
import java.sql.Driver;
import java.util.Properties;
import java.util.ResourceBundle;
import liquibase.Scope;
import liquibase.configuration.LiquibaseConfiguration;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;

public class SingleStoreConnection extends JdbcConnection {
    private final DatabaseConnection originalConnection;

    public SingleStoreConnection() {
        super();
        this.originalConnection = null;
    }

    public SingleStoreConnection(Connection connection) {
        super(connection);
        this.originalConnection = null;
    }

    /**
     * Constructor for creating a replacement connection that wraps an original connection.
     * When this connection is closed, the original connection will also be closed.
     */
    SingleStoreConnection(Connection connection, DatabaseConnection originalConnection) {
        super(connection);
        this.originalConnection = originalConnection;
    }

    @Override
    public int getPriority() {
        // Use maximum priority so this connection class is used for SingleStore
        return Integer.MAX_VALUE;
    }

    @Override
    public void close() throws DatabaseException {
        // Also close the original connection that this connection replaced.
        // This handles the case where a user manually creates a connection and passes it to
        // Liquibase. We create a replacement connection with connectionAttributes, but need
        // to ensure the original connection is also cleaned up.
        if (originalConnection != null && !originalConnection.isClosed()) {
            originalConnection.close();
        }
        super.close();
    }

    @Override
    public void open(String url, Driver driverObject, Properties driverProperties)
            throws DatabaseException {
        // This method is called by Liquibase when a new connection is needed, BEFORE the
        // connection is actually established. We append connectionAttributes to the URL
        // so SingleStore can track that this connection is from Liquibase.
        if (url != null && url.startsWith("jdbc:singlestore") && !url.contains("connectionAttributes=")) {
            url = addConnectionAttributesToUrl(url);
            
            String liquibaseVersion = getLiquibaseVersion();
            Scope.getCurrentScope().getLog(getClass()).info(
                "Connecting to SingleStore with attributes: _connector_name=Liquibase, _connector_version=" + liquibaseVersion);
        }
        
        // Let parent class handle the actual connection creation with our modified url
        super.open(url, driverObject, driverProperties);
    }

    /**
     * Adds Liquibase connection attributes to a SingleStore JDBC URL.
     * This is a static method so it can be called from SingleStoreDatabase.setConnection()
     * for manually-created connections.
     */
    static String addConnectionAttributesToUrl(String url) {
        String liquibaseVersion = getLiquibaseVersion();
        String connectionAttributes = "_connector_name:Liquibase,_connector_version:" + liquibaseVersion;
        
        String separator = url.contains("?") ? "&" : "?";
        
        if (url.contains("connectionAttributes=")) {
            // Append to existing connectionAttributes
            return url.replaceFirst(
                "(connectionAttributes=[^&]*)",
                "$1," + connectionAttributes
            );
        } else {
            // Add new connectionAttributes parameter
            return url + separator + "connectionAttributes=" + connectionAttributes;
        }
    }

    private static String getLiquibaseVersion() {
        try {
            // Try to get version from Liquibase core's build info
            String version = liquibase.util.LiquibaseUtil.getBuildVersion();
            if (version != null && !version.isEmpty() && !version.equals("DEV")) {
                return version;
            }
        } catch (Exception e) {
            // Ignore and try alternative methods
        }
        
        try {
            // Fallback: Read from liquibase core's build properties
            ResourceBundle coreBundle = ResourceBundle.getBundle("liquibase/build");
            return coreBundle.getString("build.version");
        } catch (Exception e) {
            return "unknown";
        }
    }
}
