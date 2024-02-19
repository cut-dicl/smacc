package edu.cut.smacc.server.main;

import edu.cut.smacc.cli.SmaccCLI;
import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.main.request.ClientConnectionHandler;
import edu.cut.smacc.server.statistics.StatisticsManager;
import edu.cut.smacc.server.tier.TierManager;
import edu.cut.smacc.utils.BasicGlobalTimer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Handles a new connection, whatever that connection may be (client or not)
 *
 * @author Theodoros Danos
 */
public class ServerMain {
    private static final Logger logger = LogManager.getLogger(ServerMain.class);

    private static long connectionCounter = 1;
    private static ServerSocket ssocket = null;
    private static boolean alive = true;

    private static final Map<String, ClientConnectionHandler> clientResetConnectionHandlers = new HashMap<>();
    private static final Map<String, Boolean> manualUploadingStore = new HashMap<>();

    private static String SERVER_CONFIGURATION_PATH = "conf/server.config.properties";

    public static void storeClientConnHandler(ClientConnectionHandler handler, long connectionId) {
        synchronized (clientResetConnectionHandlers) {
            if (logger.isDebugEnabled()) logger.info("Connection Stored: " + connectionId);
            clientResetConnectionHandlers.put(String.valueOf(connectionId), handler);
        }
    }

    /**
     * After some repeated timeouts the server will detach this handler with client's connection id
     *
     * @param connectionId - the client's unique connection identification number
     */
    public static void removeClientConnectionHandler(long connectionId) {
        synchronized (clientResetConnectionHandlers) {
            if (clientResetConnectionHandlers.containsKey(String.valueOf(connectionId))) {
                if (logger.isDebugEnabled()) logger.info("Connection Removed: " + connectionId);
                clientResetConnectionHandlers.remove(String.valueOf(connectionId));
            }
        }
    }

    public static boolean wakeClientConnectionHandler(long connectionId, Socket socket, DataOutputStream sout,
                                               DataInputStream sin) {
        synchronized (clientResetConnectionHandlers) {
            String cid = String.valueOf(connectionId);
            if (clientResetConnectionHandlers.containsKey(cid)) {
                clientResetConnectionHandlers.get(cid).wakeUp(socket, sout, sin);
                return true;
            } else {
                System.out.println("Not Found Connection ID(" + connectionId + ")");
                clientResetConnectionHandlers.keySet().forEach(i -> System.out.println("KEY: " + i));

                return false;
            }
        }
    }

    public static boolean hasConnectionId(long connectionId) {
        synchronized (clientResetConnectionHandlers) {
            return clientResetConnectionHandlers.containsKey(String.valueOf(connectionId));
        }
    }

    public static void storeClientManualUpload(long connectionId) {
        synchronized (manualUploadingStore) {
            manualUploadingStore.put(String.valueOf(connectionId), true);
        }
    }

    public static void removeClientManualUpload(long connectionId) {
        synchronized (manualUploadingStore) {
            manualUploadingStore.remove(String.valueOf(connectionId));
        }
    }

    public static boolean isManualUploadFinished(long connectionId) {
        synchronized (manualUploadingStore) {
            return !manualUploadingStore.containsKey(String.valueOf(connectionId));
        }
    }

    static void setServerConfigurationPath(String path) {
        SERVER_CONFIGURATION_PATH = path;
    }

    public static void main(String[] args) {
        /* Start Cache Management */
        File file = new File("conf/log4j.properties");
        if (file.exists()) {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            context.setConfigLocation(file.toURI());
        }

        // Check for CLI arguments
        SmaccCLI serverMainCLI = new ServerMainCLI(args);
        if (serverMainCLI.hasArguments()) {
            if (serverMainCLI.hasHelpArgument() || serverMainCLI.invalidArguments()) {
                System.exit(0);
            }
        }

        String serverConfigurationsFilename = SERVER_CONFIGURATION_PATH;
        File configFile = new File(serverConfigurationsFilename);
        if (!configFile.exists()) {
            logger.error("Server Configuration file does not exist: " + serverConfigurationsFilename);
            System.exit(1);
        }

        Configuration configuration = new Configuration(serverConfigurationsFilename);
        TierManager tier = new TierManager(configuration);
        StatisticsManager statisticsManager = new StatisticsManager(configuration, tier.getMemoryStatistics(),
                tier.getDiskStatistics(), tier.getStoragePerformanceStatistics());

        ExecutorService clientHandlingService = Executors
                .newFixedThreadPool(ServerConfigurations.getClientHandlingThreadPoolSize());

        /* Start Server */
        try {
            ssocket = new ServerSocket(ServerConfigurations.getServerPort(), ServerConfigurations.getServerQueueSize());
        } catch (IOException e) {
            logger.fatal("Main server failed to start: " + e.getMessage());
            System.exit(1);
        }

        System.out.println("SMACC Server has started running");
        BasicGlobalTimer.startTimer();
        /* Accept Connections */
        while (!ssocket.isClosed()) {
            try {
                Socket connection = ssocket.accept();
                connection.setSendBufferSize(ServerConfigurations.getServerBufferSize());
                connection.setSoTimeout(ServerConfigurations.getServerReadTimeout());

                logger.info("Request Accepted");
                clientHandlingService.submit(new ClientConnectionHandler(connection, tier, statisticsManager, connectionCounter++));
            } catch (SocketException se) {
                if (alive) {
                    logger.error("Server Socket Exception: " + se.getMessage());
                } else {
                    logger.info("Server Socket Closed");
                }
            } catch (IOException e) {
                logger.error("Cache Client Connection Failed: " + e.getMessage());
            }
        }

        /* Shutdown Server */
        try {
            clientHandlingService.shutdown(); // Disable new tasks from being submitted
            if (!clientHandlingService.awaitTermination(10, TimeUnit.SECONDS)) {
                if (!clientHandlingService.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.error("Client handling tasks did not terminate gracefully.");
                }
            }
            tier.shutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("SMACC Server has been shutdown successfully");
        System.exit(0);
    }

    public static void shutdown() throws IOException {
        alive = false;
        ssocket.close();
    }

}
