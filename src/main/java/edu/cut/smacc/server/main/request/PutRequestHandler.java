package edu.cut.smacc.server.main.request;

import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.main.ServerMain;
import edu.cut.smacc.server.protocol.HeaderServer;
import edu.cut.smacc.server.s3.S3File;
import edu.cut.smacc.server.tier.CacheOutputStream;
import edu.cut.smacc.server.tier.result.PutResult;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Request handler for SMACC PUT requests.
 */
public class PutRequestHandler extends RequestHandlerBase {

    protected PutRequestHandler(ClientConnectionHandler clientConnectionHandler) {
        super(clientConnectionHandler);
    }

    @Override
    public void handleRequest(HeaderServer header, CloudInfo cloudInfo) throws IOException {
        if (header.hasConnectionId() && !header.hasGoingManual()) {
            if (!ServerMain.wakeClientConnectionHandler(header.getConnectionId(), connectionHandler.getConnection(),
                    connectionHandler.getDataOutputStream(), connectionHandler.getDataInputStream())) {
                connectionHandler.closeConnection();
            }
            // if connection id is found, we wish not to close the connection
            // because another thread will serve the client
        } else if (header.hasGoingManual() && header.hasConnectionId()) {
            long conId = header.getConnectionId();
            if (!ServerMain.hasConnectionId(conId)) {
                connectionHandler.sendErrorMessage("Server reached max waiting - PUT aborted");
                connectionHandler.closeConnection();
            } else {
                if (ServerMain.isManualUploadFinished(conId)) {
                    connectionHandler.sendSuccessMessage();
                    ServerMain.wakeClientConnectionHandler(header.getConnectionId(), connectionHandler.getConnection(),
                            connectionHandler.getDataOutputStream(), connectionHandler.getDataInputStream());
                } else {
                    if (logger.isDebugEnabled()) logger.info("Connection-notify: Manual Uploading in progress");
                    connectionHandler.sendGoingManualStatus(conId);
                    connectionHandler.closeConnection();
                }
            }
        } else if (header.hasGoingManual()) {
            connectionHandler.sendErrorMessage("Protocol error: request going manual but no connection id found");
            connectionHandler.closeConnection();
        } else {
            put(header, cloudInfo);
        }
    }

    private void put(HeaderServer header, CloudInfo cloudInfo) throws IOException {
        long startTime = System.currentTimeMillis();

        String bucket = header.getBucket();
        String key = header.getKey();
        boolean async = header.getUploadAsync();
        Long length = header.getLength();

        boolean goingManual;

        PutResult result;
        CacheOutputStream out;  // S3 & Cache OutputStream
        try {   // Create a cache file
            result = tier.create(bucket, key, async, length, cloudInfo);
            out = result.getCacheOutputStream();
        } catch (Exception e) { // Cache Error Occurred
            connectionHandler.sendErrorMessage(e.getMessage());
            connectionHandler.closeConnection();
            throw new IOException(e);
        }

        try {   // Send the Client's connection id
            connectionHandler.sendSuccessMessage(connectionHandler.getConnectionId());
        } catch (IOException e) {
            connectionHandler.closeConnection();
            out.abort();
            try {
                out.close();
            } catch (IOException ignored) {
            } /* closing is necessary in order to delete incomplete cache files */
            return;
        }

        byte[] dataBuffer;
        if (length != null && length.longValue() > 0 && length.longValue() < ServerConfigurations.getServerBufferSize())
            dataBuffer = new byte[(int) length.longValue()];
        else
            dataBuffer = new byte[ServerConfigurations.getServerBufferSize()];

        while (true) try {
            DataInputStream cin = connectionHandler.getDataInputStream();
            DataOutputStream cout = connectionHandler.getDataOutputStream();

            int dataLen = cin.readInt();

            if (dataLen > 0) {
                while (dataLen > 0) {
                    int dataToRead = Math.min(dataLen, dataBuffer.length);
                    cin.readFully(dataBuffer, 0, dataToRead);
                    dataLen -= dataToRead;
                    try {
                        if (out.isGoingManual()) {
                            /* Notify Client that server is going for manual uploading */
                            cout.write(1);
                            connectionHandler.closeConnection();
                            goingManual = true;
                            /* Enqueue the thread so client can connect back */
                            ServerMain.storeClientConnHandler(connectionHandler, connectionHandler.getConnectionId());
                            /*
                             * Enqueue manualUpload status. While there is a status in queue, thread is not
                             * finished
                             */
                            ServerMain.storeClientManualUpload(connectionHandler.getConnectionId());

                            if (logger.isDebugEnabled())
                                logger.info("Going Manual");
                        } else {
                            goingManual = false;
                        }
                        out.write(dataBuffer, 0, dataToRead); // Write may block for unknown time due to manual
                                                              // uploading

                        if (goingManual) {

                            /* Notify client indirectly manual upload is finished */
                            ServerMain.removeClientManualUpload(connectionHandler.getConnectionId());
                            // Wait client to re-connect
                            // If client reached the limit of waiting, then abort
                            if (!connectionHandler.isClientConnected()) {
                                try {
                                    out.abort();
                                    out.close();
                                } catch (IOException ignored) {
                                }
                                return;
                            }
                        } else {
                            if (dataLen == 0) {
                                // We should only send success message in case it's not a client coming from
                                // manual uploading (because in that case the message it is already sent by
                                // another thread)
                                cout.write(0);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        cout.write(2);
                        connectionHandler.closeConnection();
                        return;
                    }
                }
            } else {
                /* EOF Occurred - Close Cache File */
                if (logger.isDebugEnabled()) logger.info("EOF Received");

                /* Asynchronous Task - Close file and async threads will handle the file from now on */
                if (out.isAsync()) {
                    if (logger.isDebugEnabled()) logger.info("Async - closing");
                    if (out.isNotClosed()) out.close();
                    connectionHandler.sendSuccessMessage();
                    connectionHandler.closeConnection();
                    return;
                }

                if (out.isNotClosed()) {

                    try {
                        out.uploadLastPart();
                    } catch (Exception e) {
                        logger.error("Uploading Last Part EXCEPTION - ERROR IN UPLOADING");
                        logger.error(e);
                        out.abort();
                        try {
                            out.close();
                        } catch (IOException ignored) {
                        } /* closing is necessary in order to delete incomplete cache files */
                        try {
                            connectionHandler.sendErrorMessage(e.getMessage());
                        } catch (IOException ignored) {
                        }
                        return;
                    }

                    /* Non-Block -  While file is flushing to s3 - wait for it and meanwhile send messages to avoid read timeout */
                    while (out.isUploading()) {
                        if (logger.isDebugEnabled()) logger.info("Still Uploading!");
                        connectionHandler.sendKeepAlive();
                        try {
                            Thread.sleep(ServerConfigurations.getKeepAliveTime());
                        } catch (InterruptedException ignored) {
                        }
                    }

                    if (length != null && !async) {
                        while (out.isUploadingWithLength()) {
                            if (logger.isDebugEnabled()) logger.info("Still Uploading!");
                            connectionHandler.sendKeepAlive();
                            try {
                                out.waitUploadWithLength(ServerConfigurations.getKeepAliveTime());
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }


                    if (logger.isDebugEnabled()) logger.info("All uploaders finished OK - Closing");

                    if (connectionHandler.getConnection().isConnected()) try {
                        out.close();
                    } catch (IOException e) {
                        connectionHandler.sendErrorMessage(e.getMessage());
                        return;
                    }
                }
                connectionHandler.sendSuccessMessage();
                long putTime = System.currentTimeMillis() - startTime;
                timeStatUpdater.updateOnPut(0, putTime);
                for (CacheFile cacheFile : out.getCacheFiles()) {
                    // Update TIER statistics for: SIZE, TIME, COUNT
                    if (cacheFile.getStoreOption() == StoreOptionType.MEMORY_ONLY) {
                        memoryStatUpdater.updateOnPut(cacheFile.getTotalSize(), putTime);
                    } else if (cacheFile.getStoreOption() == StoreOptionType.DISK_ONLY) {
                        diskStatUpdater.updateOnPut(cacheFile.getSize(), putTime);
                    }
                }
                S3File s3File = (S3File) result.getS3File();
                if (s3File != null) {
                    // Update S3 statistics for: SIZE, TIME, COUNT
                    s3StatUpdater.updateOnPut(s3File.getActualSize(), putTime);
                }
                ServerMain.removeClientConnectionHandler(connectionHandler.getConnectionId());
                break;
            }
        } catch (IOException e) {
            out.abort();
            System.err.println("Client Disconnected with error: " + e.getMessage());
            connectionHandler.closeConnection();
            break;
        }

        ServerMain.removeClientConnectionHandler(connectionHandler.getConnectionId());
        connectionHandler.closeConnection();
    }
}
