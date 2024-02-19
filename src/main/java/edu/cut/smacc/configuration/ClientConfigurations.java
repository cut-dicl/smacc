package edu.cut.smacc.configuration;

/**
 * Default configurations and loading of user configurations for client
 * 
 * @author everest
 *
 */
public class ClientConfigurations extends BaseConfigurations {

    // user configurable
    private static int CLIENT_READ_TIMEOUT_MS;
    private static int CLIENT_CONNECTION_TIMEOUT_MS;
    private static int CLIENT_BUFFER_SIZE;
    private static int CLIENT_MAX_RECONNECT_WAIT_TIME_MS; // How many ms to wait before attempt to connect again

    private static int CLIENT_MAX_RETRIES_ALLOWED;
    private static boolean CLIENT_MODE_ASYNC;
    private static int MANUAL_UPLOAD_RECONNECT_WAIT_MS;
    private static double CLIENT_READ_WRITE_PROTOPRIONAL_WAIT;

    private static String[] SERVERS_LIST = { "127.0.0.1" };
    private static int SERVERS_PORT;

    /**
     * Keys, descriptions and default values for the configuration file
     */

    public static final String CLIENT_READ_TIMEOUT_MS_KEY = "client.read.timeout.ms";
    public static final String CLIENT_READ_TIMEOUT_MS_DESCRIPTION = "Read timeout for the client in milliseconds";
    public static int CLIENT_READ_TIMEOUT_MS_DEFAULT = 20000;

    public static final String CLIENT_CONNECTION_TIMEOUT_MS_KEY = "client.connection.timeout.ms";
    public static final String CLIENT_CONNECTION_TIMEOUT_MS_DESCRIPTION = "Connection timeout for the client in milliseconds";
    public static int CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT = 15000;

    public static final String CLIENT_BUFFER_SIZE_KEY = "client.buffer.size.bytes";
    public static final String CLIENT_BUFFER_SIZE_DESCRIPTION = "Buffer size for the client in bytes";
    public static int CLIENT_BUFFER_SIZE_DEFAULT = 2 * 1024 * 1024;

    public static final String CLIENT_MAX_RECONNECT_WAIT_TIME_MS_KEY = "client.reconnect.wait.ms";
    public static final String CLIENT_MAX_RECONNECT_WAIT_TIME_MS_DESCRIPTION = "Maximum time to wait before attempting to reconnect to the server in milliseconds";
    public static int CLIENT_MAX_RECONNECT_WAIT_TIME_MS_DEFAULT = 4000;

    public static final String CLIENT_MAX_RETRIES_ALLOWED_KEY = "client.reconnect.retries";
    public static final String CLIENT_MAX_RETRIES_ALLOWED_DESCRIPTION = "Maximum number of retries to attempt to reconnect to the server";
    public static int CLIENT_MAX_RETRIES_ALLOWED_DEFAULT = 5;

    public static final String CLIENT_MODE_ASYNC_KEY = "client.mode.asynchronous.put";
    public static final String CLIENT_MODE_ASYNC_DESCRIPTION = "Whether to use asynchronous put or not";
    public static boolean CLIENT_MODE_ASYNC_DEFAULT = false;

    public static final String MANUAL_UPLOAD_RECONNECT_WAIT_MS_KEY = "client.manualUpload.wait.ms";
    public static final String MANUAL_UPLOAD_RECONNECT_WAIT_MS_DESCRIPTION = "How many ms to wait before attempting to reconnect to the server when uploading a file manually";
    public static int MANUAL_UPLOAD_RECONNECT_WAIT_MS_DEFAULT = 150;

    public static final String CLIENT_READ_WRITE_PROTOPRIONAL_WAIT_KEY = "client.readwrite.proportionalwait.double";
    public static final String CLIENT_READ_WRITE_PROTOPRIONAL_WAIT_DESCRIPTION = "Proportional wait time for read and write operations";
    public static double CLIENT_READ_WRITE_PROTOPRIONAL_WAIT_DEFAULT = 3;

    public static final String SERVERS_LIST_KEY = "smacc.servers.list";
    public static final String SERVERS_LIST_DESCRIPTION = "List of servers to connect to";
    public static final String[] SERVERS_LIST_DEFAULT = { "127.0.0.1" };

    public static final String SERVERS_PORT_KEY = "smacc.servers.port";
    public static final String SERVERS_PORT_DESCRIPTION = "Port to connect to";
    public static int SERVERS_PORT_DEFAULT = 1111;


    public static int getClientReadTimeout() {
        return CLIENT_READ_TIMEOUT_MS;
    }

    public static int getClientConnectionTimeout() {
        return CLIENT_CONNECTION_TIMEOUT_MS;
    }

    public static int getClientBufferSize() {
        return CLIENT_BUFFER_SIZE;
    }

    public static int getClientReconnectWaitMs() {
        return CLIENT_MAX_RECONNECT_WAIT_TIME_MS;
    }

    public static int getServerMaxRetries() {
        return CLIENT_MAX_RETRIES_ALLOWED;
    }

    public static boolean getClientModeAsync() {
        return CLIENT_MODE_ASYNC;
    }

    public static String[] getServerList() {
        return SERVERS_LIST;
    }

    public static int getServersPort() {
        return SERVERS_PORT;
    }

    public static int getManualUploadingWaitMs() {
        return MANUAL_UPLOAD_RECONNECT_WAIT_MS;
    }

    public static double getReadWriteProportionalWait() {
        return CLIENT_READ_WRITE_PROTOPRIONAL_WAIT;
    }


    public static void initialize(Configuration conf) throws ConfigurationException {
        BaseConfigurations.initialize(conf);

        CLIENT_READ_TIMEOUT_MS = conf.getInt(CLIENT_READ_TIMEOUT_MS_KEY, CLIENT_READ_TIMEOUT_MS_DEFAULT);
        CLIENT_CONNECTION_TIMEOUT_MS = conf.getInt(CLIENT_CONNECTION_TIMEOUT_MS_KEY,
                CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT);
        CLIENT_BUFFER_SIZE = conf.getInt(CLIENT_BUFFER_SIZE_KEY, CLIENT_BUFFER_SIZE_DEFAULT);
        CLIENT_MAX_RECONNECT_WAIT_TIME_MS = conf.getInt(CLIENT_MAX_RECONNECT_WAIT_TIME_MS_KEY,
                CLIENT_MAX_RECONNECT_WAIT_TIME_MS_DEFAULT);
        CLIENT_MAX_RETRIES_ALLOWED = conf.getInt(CLIENT_MAX_RETRIES_ALLOWED_KEY, CLIENT_MAX_RETRIES_ALLOWED_DEFAULT);
        CLIENT_MODE_ASYNC = conf.getBoolean(CLIENT_MODE_ASYNC_KEY, CLIENT_MODE_ASYNC_DEFAULT);
        MANUAL_UPLOAD_RECONNECT_WAIT_MS = conf.getInt(MANUAL_UPLOAD_RECONNECT_WAIT_MS_KEY,
                MANUAL_UPLOAD_RECONNECT_WAIT_MS_DEFAULT);
        CLIENT_READ_WRITE_PROTOPRIONAL_WAIT = conf.getDouble(CLIENT_READ_WRITE_PROTOPRIONAL_WAIT_KEY,
                CLIENT_READ_WRITE_PROTOPRIONAL_WAIT_DEFAULT);

        if (conf.containsKey(SERVERS_LIST_KEY)) {
            SERVERS_LIST = conf.getStringArray(SERVERS_LIST_KEY);
        }
        SERVERS_PORT = conf.getInt(SERVERS_PORT_KEY, SERVERS_PORT_DEFAULT);
    }

}
