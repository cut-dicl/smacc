package edu.cut.smacc.client.request;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.BasicAWSCredentials;

import edu.cut.smacc.configuration.ClientConfigurations;
import edu.cut.smacc.server.protocol.RequestType;
import edu.cut.smacc.server.protocol.StatusProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This class is used when the user want to send an object to SMACC.
 * SMACCClient uses this class in putObject() method
 *
 * @author everest
 */
class SmaccClientOutputStream extends OutputStream implements SmaccClientRequest {
    private static final Logger logger = LogManager.getLogger(SmaccClientOutputStream.class);

    private String bucket;
    private String key;
    private boolean async;
    private Socket socket = null;
    private DataOutputStream sout = null;
    private DataInputStream sin = null;
    private int clientBufferSize;
    private byte[] localBuffer;
    private int localAvailable;
    private int localOffset = 0;
    private boolean isClosed = false;
    // private int i = 0;
    private long connectionId = -1;
    private BasicAWSCredentials clientCredentials;
    private MessageDigest messageDigest;
    private byte[] oneByte = new byte[1];
    private String endPoint;
    private String region;
    private Long length;

    SmaccClientOutputStream(String bucket, String key, boolean async, BasicAWSCredentials clientCredentials, String endPoint, String region, Long length) {
        this.async = async;
        this.bucket = bucket;
        this.key = key;
        this.clientCredentials = clientCredentials;
        this.region = region;
        this.endPoint = endPoint;
        this.length = length;

        this.clientBufferSize = ClientConfigurations.getClientBufferSize();
        if (length != null && length.longValue() > 0 && length.longValue() < clientBufferSize)
            this.clientBufferSize = (int) length.longValue();
        this.localBuffer = new byte[clientBufferSize];
        this.localAvailable = clientBufferSize;

    }

    public boolean initiate() {
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new SdkClientException("MD5 Algorithm not supported by current JRE");
        }

        int retriesMade = 0;
        do {
            try {
                /* Connect to cache server and Initiate PUT Request */
                connect(false);
                retriesMade = 0;
                break;
            } catch (IOException ex) {
                logger.error("Initial Connection Lost: " + ex.getMessage());
                ServerDial.disconnect(socket);
                retriesMade += 1;
                try {
                    Thread.sleep(ClientConfigurations.getClientReconnectWaitMs());
                } catch (InterruptedException ignored) {
                }
            }
        }
        while (retriesMade <= ClientConfigurations.getServerMaxRetries());

        if (retriesMade > 0)
            throw new SdkClientException("Max Retries Depleted - Server is unreachable");

        return true;
    }

    public void write(int c) throws IOException {
        if (isClosed) throw new IOException("Writting to a closed stream");

        oneByte[0] = (byte) c;
        write(oneByte, 0, 1);
    }

    public void write(byte[] buffer) throws IOException {
        write(buffer, 0, buffer.length);
    }

    public void write(byte[] buffer, int offset, int len) throws IOException {
        if (isClosed) throw new IOException("Writting to a closed stream");

        messageDigest.update(buffer, offset, len);

        if (localOffset == clientBufferSize) {
            sendLocalBuffer(clientBufferSize);
        }

        if (localAvailable >= len) {
            System.arraycopy(buffer, offset, localBuffer, localOffset, len);
            localAvailable -= len;
            localOffset += len;
        } else {
            int bufferLen = len, cpyLen, bufferOffset = offset;
            while (bufferLen > 0) {
                if (localAvailable > bufferLen)
                    cpyLen = bufferLen;
                else
                    cpyLen = localAvailable;

                System.arraycopy(buffer, bufferOffset, localBuffer, localOffset, cpyLen);
                localAvailable -= cpyLen;
                localOffset += cpyLen;
                bufferLen -= cpyLen;
                bufferOffset += cpyLen;

                if (localOffset == clientBufferSize) {
                    sendLocalBuffer(clientBufferSize);
                }
            }
        }
    }

    /* Closes the connection with smacc server */
    public void close() throws IOException {
        if (isClosed)
            return;

        isClosed = true;
        if (localOffset > 0)
            sendLocalBuffer(localOffset);

        StatusProtocol status;
        int retries = 0;

        // ++i;

        try {
            do {
                try {
                    int eof = -1;
                    sout.writeInt(eof);
                    sout.flush();

                    /* Receive Success/Error Message */
                    status = new StatusProtocol(sin);

                    if (status.getFailure()) {
                        throw new Exception(status.getExceptionMessage());
                    }

                    while (status.hasKeepAlive()) {
                        status.read(sin);

                        if (status.getFailure()) {
                            throw new Exception(status.getExceptionMessage());
                        }
                    }

                    retries = 0;
                    break;
                } catch (IOException e) {
                    ServerDial.disconnect(socket);
                    e.printStackTrace();
                    retries += 1;
                    try {
                        Thread.sleep(ClientConfigurations.getClientReconnectWaitMs());
                    } catch (InterruptedException ignored) {
                    }
                }
            } while (retries <= ClientConfigurations.getServerMaxRetries());
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            ServerDial.disconnect(socket);
        }

        if (retries > 0)
            throw new IOException("Repeated failure - Failed to connect with Cache Server");
    }

    public String getETag() {
        // Get the calculated MD5 hash
        byte[] md5Bytes = messageDigest.digest();

        // Convert the MD5 bytes to a hexadecimal string
        StringBuilder sb = new StringBuilder();
        for (byte md5Byte : md5Bytes) {
            sb.append(String.format("%02x", md5Byte));
        }

        // Return the ETag value
        return sb.toString();
    }

    /**
     * When the local buffer is full (or there are the last data) send the data of buffer to smacc server
     *
     * @param len
     * @throws IOException
     */
    private void sendLocalBuffer(int len) throws IOException {
        // ++i;
        try {

            sout.writeInt(len);
            sout.write(localBuffer, 0, len);

            /*
             * We always receive message because of out of memory,
             * may return going manual when in asynchronous mode,
             * so we must be able to catch that.
             * 
             * Receive Success/Error Message
             */
            int b = sin.read();

            if (b == 1) { // Going manual
                async = false;
                /* Confirm reception of Going Manual message */

                ServerDial.disconnect(socket);
                repeatedWaitManualUpload();
            }

            localAvailable = clientBufferSize;
            localOffset = 0;

            if (b > 1) {
                throw new Exception("Cache Server wtih status code: " + b);
            }
        } catch (IOException e) {
            ServerDial.disconnect(socket);
            logger.error("Connection Lost: " + e.getMessage());
            throw new IOException(e);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * This method calls the smacc server repeatedly to find out whether has done the uploading or not
     *
     * @throws IOException
     * @throws Exception
     */
    private void repeatedWaitManualUpload() throws IOException, Exception {
        StatusProtocol status;
        while (true) {
            /* Connect/Reconnect to Server */
            socket = ServerDial.connect();
            sout = new DataOutputStream(socket.getOutputStream());
            sin = new DataInputStream(socket.getInputStream());

            /* Send Login Message */
            ServerDial.login(sout, clientCredentials, endPoint, region);

            /* Send Header */
            System.out.println("Send PUT Header");
            writePutRequestHeader(sout, bucket, key, async, connectionId, true, length);

            /* Give time to server to open the file */
            socket.setSoTimeout((int) (ClientConfigurations.getClientReadTimeout() * ClientConfigurations.getReadWriteProportionalWait()));

            /* Receive status */
            status = new StatusProtocol(sin);

            socket.setSoTimeout(ClientConfigurations.getClientReadTimeout());

            /* Find if there was an error */
            if (status.getFailure())
                throw new Exception(status.getExceptionMessage());

            /* If server is finished it will not return goingManual field, but only pure status success */
            if (!status.hasGoingManual())
                break;

            System.out.println("Waiting for Manual Upload...");

            /* if server is still manual uploading disconnect and try to sleep until reconnect later */
            ServerDial.disconnect(socket);
            try {
                Thread.sleep(ClientConfigurations.getManualUploadingWaitMs());
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * Send the necessary data in order to negotiate with the server for a put request
     *
     * @param bucket           - the bucket to store the object
     * @param key              - the key of the object we wish to write
     * @param reconnect        - in case previous call failed
     * @param waitManualUpload - in case we reconnect for manual uploading
     * @throws SdkClientException
     * @throws AmazonServiceException
     */
    private void initiatePutRequest(String bucket, String key, boolean reconnect, boolean waitManualUpload, Long length) throws SdkClientException, AmazonServiceException {
        StatusProtocol status;
        System.out.println("Send PUT Request for " + key);
        try {
            /* Send Header */
            if (reconnect) {
                writePutRequestHeader(sout, bucket, key, async, connectionId, waitManualUpload, length);
            } else {
                writePutRequestHeader(sout, bucket, key, async, null, waitManualUpload, length);
            }
        } catch (IOException e) {
            throw new SdkClientException(e);
        }

        try {
            /* Give time to server to open the file */
            socket.setSoTimeout((int) (ClientConfigurations.getClientReadTimeout() * ClientConfigurations.getReadWriteProportionalWait()));
        } catch (SocketException e) {
            throw new SdkClientException(e);
        }

        /* Receive Success/Error Message - Needed for client or server errors */
        // System.out.println("Wait Header Message #" + i);
        try {
            status = new StatusProtocol(sin);
        } catch (IOException e) {
            throw new SdkClientException(e);
        }

        try {
            socket.setSoTimeout(ClientConfigurations.getClientReadTimeout());
        } catch (SocketException e) {
            throw new SdkClientException(e);
        }

        if (status.getFailure())
            throw new AmazonServiceException("Server Error: " + status.getExceptionMessage());

        /* Throw Server Exception (Exception) if connectionId is not included (is part of put negotiation) */
        if (!status.hasConnectionId())
            throw new AmazonServiceException("Protocol Error - Should never happen  [status.connectionId is missing]");

        /* Get Connection id - Unique id from server. In case of connection reset,
         * the connection id will be reused in order to continue to write to the same output */
        connectionId = status.getConnectionId();
    }

    /**
     * Connects to the server
     *
     * @param reconnect - is true when the previous attempt failed
     * @throws IOException
     */
    private void connect(boolean reconnect) throws IOException {
        /* Connect/Reconnect to Server */
        // System.out.println("Connecting...");
        socket = ServerDial.connect();
        sout = new DataOutputStream(socket.getOutputStream());
        sin = new DataInputStream(socket.getInputStream());

        /* Send Login Message */
        ServerDial.login(sout, clientCredentials, endPoint, region);

        /* Initiate Put Request */
        initiatePutRequest(bucket, key, reconnect, false, length);
    }

    private static void writePutRequestHeader(DataOutputStream out, String bucket, String key, boolean async,
                                              Long connectionId, boolean goingManual, Long length) throws IOException {
        out.write(RequestType.PUT.getInt());
        out.write(bucket.length());
        out.write(bucket.getBytes());
        if (key == null) {
            out.write(0);
        } else {
            out.write(key.length());
            out.write(key.getBytes());
        }
        out.writeBoolean(async);
        if (connectionId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(connectionId);
        }
        out.writeBoolean(goingManual);
        if (length == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(length);
        }

        out.flush();
    }
}
