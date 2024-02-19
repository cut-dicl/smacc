package edu.cut.smacc.server.main.request;

import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.main.ServerMain;
import edu.cut.smacc.server.protocol.HeaderServer;
import edu.cut.smacc.server.protocol.RequestType;
import edu.cut.smacc.utils.BasicGlobalTimer;
import edu.cut.smacc.server.statistics.StatisticsManager;
import edu.cut.smacc.server.tier.TierManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.Socket;

/**
 * Handles the connection after class ServerMain accepted the connection of a client
 * The class has to negotiate with the client using the smacc protocol
 * The SMACC Protocol is implemented inside this class
 *
 * @author Theodoros Danos
 */
public class ClientConnectionHandler implements Runnable {
    private static final Logger logger = LogManager.getLogger(ClientConnectionHandler.class);

    private Socket connection;
    private final TierManager tier;
    private final StatisticsManager statisticsManager;
    private final long connectionId;
    private DataInputStream cin;
    private DataOutputStream cout;
    private volatile boolean clientReconnected = false;

    public ClientConnectionHandler(Socket connection, TierManager tier, StatisticsManager statisticsManager, long connectionId) {
        this.connection = connection;
        this.statisticsManager = statisticsManager;
        this.tier = tier;
        this.connectionId = connectionId;
    }

    TierManager getTier() {
        return tier;
    }

    StatisticsManager getStatisticsManager() {
        return statisticsManager;
    }

    long getConnectionId() {
        return connectionId;
    }

    DataInputStream getDataInputStream() {
        return cin;
    }

    DataOutputStream getDataOutputStream() {
        return cout;
    }

    Socket getConnection() {
        return connection;
    }

    @Override
    public void run() {
        try {
            /* Waiting for S3 Login Credentials Message */
            cin = new DataInputStream(connection.getInputStream());
            cout = new DataOutputStream(connection.getOutputStream());

            if (logger.isDebugEnabled()) logger.info("Waiting for Login Message");

            String accessKey = new String(cin.readNBytes(cin.read()));
            String secretKey = new String(cin.readNBytes(cin.read()));
            String endPoint = new String(cin.readNBytes(cin.read()));
            String region = new String(cin.readNBytes(cin.read()));
            CloudInfo cloudInfo = new CloudInfo(endPoint, region, accessKey, secretKey);

            /* Get Communication Header */
            if (logger.isDebugEnabled()) logger.info("Waiting for Header Message");
            HeaderServer comHeader = new HeaderServer(cin);
            if (!comHeader.hasRequestType()) {
                sendErrorMessage("Request Type field is missing from message");
                logger.error("Request Type field is missing from message");
                return;
            }

            if (logger.isDebugEnabled()) logger.info("-----> Header Type: " + comHeader.getRequestType().name());

            /* Handle the request */
            handleRequest(comHeader, cloudInfo);
            if (logger.isDebugEnabled()) logger.info("Client Session Closed");

        } catch (EOFException e) { // Client closed the connection unexpectedly
            logger.info("Client Disconnected Unexpectedly");
        } catch (IOException e) {
            logger.error("RUN IOE", e);
        } catch (Exception e) {
            logger.error("RUN EXC", e);
        }
    }

    /**
     * Wake up the client when the client connects back (in case of going manual)
     * Then replace socket, socket out and in with the new ones of the connected client
     *
     * @param socket - socket with the connection of new client
     * @param sout   - socket out
     * @param sin    - socket in
     */
    public void wakeUp(Socket socket, DataOutputStream sout, DataInputStream sin) {
        if (logger.isDebugEnabled()) logger.info("Waking Connection Handler...");

        connection = socket;
        cin = sin;
        cout = sout;
        clientReconnected = true;
    }

    /**
     * Execute the appropriate action that client requested
     *
     * @param header - Communication Header - Provides which action the server should execute
     */
    private void handleRequest(HeaderServer header, CloudInfo cloudInfo) throws IOException {
        RequestType type = header.getRequestType();
        if (handleSpecialRequest(type)) {
            return;
        }
        if (statisticsManager.outputStatistics()) {
            logger.info("Outputting Statistics");
            BasicGlobalTimer.resetTimer();
        }
        if (!header.hasBucket() && type != RequestType.CLEAR_CACHE) {
            sendErrorMessage("Bucket field is missing from Message");
            return;
        }

        RequestHandler requestHandler;
        switch (type) {
            case GET -> requestHandler = RequestHandlerFactory.createGetRequestHandler(this);
            case PUT -> requestHandler = RequestHandlerFactory.createPutRequestHandler(this);
            case DEL -> requestHandler = RequestHandlerFactory.createDeleteRequestHandler(this);
            case LIST_CACHE -> requestHandler = RequestHandlerFactory.createListCacheRequestHandler(this);
            case DEL_CACHE, CLEAR_CACHE -> requestHandler = RequestHandlerFactory
                    .createDeleteCacheRequestHandler(this);
            case FILE_STATUS -> requestHandler = RequestHandlerFactory.createFileStatusRequestHandler(this);
            default -> {
                sendErrorMessage("Request Type is not supported");
                return;
            }
        }
        requestHandler.handleRequest(header, cloudInfo);
    }

    /**
     * Check if the request is a special request (e.g. stats, shutdown, clear cache) and handle the request
     * @param type - request type
     * @return true if the request is a special request, false otherwise
     * @throws IOException - if an I/O error occurs
     */
    private boolean handleSpecialRequest(RequestType type) throws IOException {
        if (type == RequestType.COLLECT_STATS) {
            statisticsManager.outputStatisticsOnClientRequest();
            return true;
        } else if (type == RequestType.RESET_STATS) {
            statisticsManager.resetStatistics();
            return true;
        }
        else if (type == RequestType.SHUTDOWN) {
            ServerMain.shutdown();
            closeConnection();
            return true;
        }
        return false;
    }

    void closeConnection() {
        try {
            if (!connection.isClosed()) connection.close();
        } catch (Exception ignored) {
        }
    }

    boolean isClientConnected() {
        int retries = 0;

        while (retries < ServerConfigurations.getManualUploadMaxRetries()) {
            retries += 1;
            if (!clientReconnected) {
                if (logger.isDebugEnabled()) logger.info("Manual Upload finished waiting for client " + retries);
                try {
                    Thread.sleep(ServerConfigurations.getManualUploadReconnectWaitMs());
                } catch (InterruptedException ignored) {
                }   // Waiting for wake up or timeout
            } else {
                if (logger.isDebugEnabled()) logger.info("Client connected back from manual upload waiting");
                clientReconnected = false;
                return true;
            }
        }
        return false;
    }

    /**
     * Send error message to client
     *
     * @param message - the exception message to send
     * @throws IOException - if the connection is closed
     */
    void sendErrorMessage(String message) throws IOException {
        sendStatus(cout, false, false, false, null, message);
    }

    /**
     * Send success message to the client
     *
     * @throws IOException - if the connection is closed
     */
    void sendSuccessMessage() throws IOException {
        sendStatus(cout, true, false, false, null, null);
    }

    /**
     * Send success message to the client
     *
     * @param connectionId - The unique connection Id of client (generated by server)
     * @throws IOException - if the connection is closed
     */
    void sendSuccessMessage(long connectionId) throws IOException {
        sendStatus(cout, true, false, false, connectionId, null);
    }

    /**
     * Send keepalive message to client
     *
     * @throws IOException - if the connection is closed
     */
    void sendKeepAlive() throws IOException {
        sendStatus(cout, true, true, false, null, null);
    }

    /**
     * tell client to go to going manual mode
     *
     * @param connectionId - The unique connection Id of client (generated by server)
     * @throws IOException - if the connection is closed
     */
    void sendGoingManualStatus(long connectionId) throws IOException {
        sendStatus(cout, true, false, true, connectionId, null);
    }

    private static void sendStatus(DataOutputStream out, boolean success,
                                   boolean keepAlive, boolean goingManual,
                                   Long connectionId, String exceptionMessage) throws IOException {
        try {
            out.writeBoolean(success);
            out.writeBoolean(keepAlive);
            out.writeBoolean(goingManual);
            if (connectionId == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeLong(connectionId);
            }
            if (exceptionMessage == null) {
                out.writeShort(0);
            } else {
                out.writeShort(exceptionMessage.length());
                out.write(exceptionMessage.getBytes());
            }
            out.flush();
        } catch (IOException e) {
            throw new IOException("Connection closed");
        }
    }
}
