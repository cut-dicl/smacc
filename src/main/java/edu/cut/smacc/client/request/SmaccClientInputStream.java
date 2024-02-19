package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.configuration.ClientConfigurations;
import edu.cut.smacc.server.cache.common.BlockRange;
import edu.cut.smacc.server.protocol.RequestType;
import edu.cut.smacc.server.protocol.StatusProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/**
 * This class is used when the user wants to get an object using SMACC.
 * SMACCClient class uses the ClientInputStream in getObject() method
 *
 * @author Theodoros Danos
 */
class SmaccClientInputStream extends InputStream implements SmaccClientRequest {
    private static final Logger logger = LogManager.getLogger(SmaccClientInputStream.class);

    private Socket socket;
    private DataInputStream sin;
    private DataOutputStream sout;
    private byte[] localBuffer;
    private boolean hasMoreData = true;
    private int localAvailable = 0;
    private int localOffset = 0;
    private BlockRange range;
    private String bucket;
    private String key;
    private boolean isClosed = false;
    private BasicAWSCredentials clientCredentials;
    private String endPoint;
    private String region;
    private long size;

    SmaccClientInputStream(String bucket, String key, BlockRange range, BasicAWSCredentials clientCredentials, String endPoint, String region) {
        this.range = range;
        this.bucket = bucket;
        this.key = key;
        this.clientCredentials = clientCredentials;
        this.endPoint = endPoint;
        this.region = region;
    }

    public boolean initiate() throws IOException {
        int retriesMade = 0;
        do {
            try {
                /* Connect/Reconnect to Server */
                socket = ServerDial.connect();
                sout = new DataOutputStream(socket.getOutputStream());
                sin = new DataInputStream(socket.getInputStream());
                ServerDial.login(sout, clientCredentials, endPoint, region);

                /* Initiate Get Request */
                initiateGetRequest(bucket, key, range);

                break;
            } catch (IOException ex) {
                logger.error("Error initiating client input stream: " + ex.getMessage());
                ServerDial.disconnect(socket);
                if (ex.getMessage() != null && ex.getMessage().contains("does not exist")) {
                    System.exit(1);
                }
                retriesMade += 1;
                if (retriesMade > ClientConfigurations.getServerMaxRetries()) throw new IOException(ex);
                try {
                    Thread.sleep(ClientConfigurations.getClientReconnectWaitMs());
                } catch (InterruptedException ignored) {
                }
            }
        }
        while (retriesMade <= ClientConfigurations.getServerMaxRetries());

        return true;
    }

    public int read() throws IOException {
        /* This function is not used by the S3's function. Although exists for completion of the class */
        if (isClosed) throw new IOException("Reading to a closed stream");
        if (localAvailable == 0) fillLocalBuffer();

        if (hasMoreData) {
            int c = localBuffer[localOffset];
            localOffset += 1;
            localAvailable -= 1;
            return c;
        } else
            return -1;
    }

    public int read(byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    public int read(byte[] buffer, int offset, int len) throws IOException {
        if (isClosed) throw new IOException("Reading to a closed stream");

        int copyLen;
        if (localAvailable == 0) fillLocalBuffer();
        copyLen = Math.min(len, localAvailable);

        if (hasMoreData) {
            System.arraycopy(localBuffer, localOffset, buffer, offset, copyLen);
            localOffset += copyLen;
            localAvailable -= copyLen;
            return copyLen;
        } else
            return -1;
    }

    public int available() throws IOException {
        if (localAvailable == 0) fillLocalBuffer();
        return localAvailable;
    }

    public long skip(long bytesToSkip) {
        if (bytesToSkip > localBuffer.length)
            return 0;
        else {
            int skippingBytes = localAvailable;
            localAvailable = 0;
            localOffset = 0;
            return skippingBytes;
        }
    }

    public boolean markSupported() {
        return false;
    }

    public void close() throws IOException {
        if (isClosed)
            return;

        isClosed = true;
        ServerDial.disconnect(socket);
    }

    /**
     * When the local buffer the user reads is empty then the fillLocalBuffer is called in order to refill the buffer or end the file
     *
     * @throws IOException
     */
    private void fillLocalBuffer() throws IOException {

        if (!hasMoreData) return;

        try {

            /* Receive Data from Cache/S3 */
            int dataLen = sin.read(localBuffer, 0, localBuffer.length);
            if (dataLen > 0) {
                localAvailable = dataLen;
                localOffset = 0;
            } else {
                /* EOF Occurred - Close Cache File */
                hasMoreData = false;
            }

        } catch (IOException ex) {
            System.out.println("Connection Lost: " + ex.getMessage());
            ServerDial.disconnect(socket);
            throw new IOException(ex);
        }
    }

    /**
     * Initiate negotiation with the smacc server in order to start reading
     *
     * @param bucket - the bucket we request to read from
     * @param key    - the key of the object we request to get
     * @param range  - the range (if there is any) of the object we wish to get
     * @throws IOException
     */
    private void initiateGetRequest(String bucket, String key, BlockRange range) throws IOException {
        StatusProtocol status;

        /* Send Header */
        System.out.println("Send GET Request for " + key);
        writeGetRequestHeader(sout, bucket, key, range);

        /* Give the server little more time just for finding file */
        socket.setSoTimeout((int) (ClientConfigurations.getClientReadTimeout() * ClientConfigurations.getReadWriteProportionalWait()));

        /* Receive Success/Error Message */
        status = new StatusProtocol(sin);

        socket.setSoTimeout(ClientConfigurations.getClientReadTimeout());

        if (status.hasExceptionMessage()) {
            throw new IOException("Server Error: " + status.getExceptionMessage());
        }

        // get file size
        size = sin.readLong();
        if (size > 0 && size < ClientConfigurations.getClientBufferSize())
            localBuffer = new byte[(int) size];
        else
            localBuffer = new byte[ClientConfigurations.getClientBufferSize()];
    }

    private static void writeGetRequestHeader(DataOutputStream out, String bucket, String key,
                                              BlockRange range) throws IOException {
        out.write(RequestType.GET.getInt());
        out.write(bucket.length());
        out.write(bucket.getBytes());
        if (key == null) {
            out.write(0);
        } else {
            out.write(key.length());
            out.write(key.getBytes());
        }
        if (range == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(range.getStart());
            out.writeLong(range.getStop());
        }
        out.flush();
    }
}