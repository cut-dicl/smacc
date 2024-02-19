package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.protocol.RequestType;
import edu.cut.smacc.server.protocol.StatusProtocol;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Send a delete request to the SMACC server
 */
public class SmaccClientDeleteRequest extends SmaccClientBucketKeyRequest {

    protected SmaccClientDeleteRequest(BasicAWSCredentials clientCredentials, String endPoint, String region,
                                       String bucket, String key) {
        super(clientCredentials, endPoint, region, bucket, key, RequestType.DEL);
    }

    @Override
    protected boolean initiateRequest() throws IOException {
        System.out.println("Send DEL Request for " + key);
        writeHeader();

        /* Receive Success/Error Message */
        DataInputStream sin = new DataInputStream(socket.getInputStream());
        StatusProtocol status = new StatusProtocol(sin);
        if (status.getFailure()) {
            if ("File not found!".equalsIgnoreCase(status.getExceptionMessage()))
                return false; // don't throw an exception for this
            else
                throw new IOException(status.getExceptionMessage());
        }

        return true;
    }

}
