package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.protocol.RequestType;
import edu.cut.smacc.server.protocol.StatusProtocol;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Client request for clearing the SMACC server cache
 */
public class SmaccClientClearCacheRequest extends SmaccClientAbstractRequest {

    SmaccClientClearCacheRequest(BasicAWSCredentials clientCredentials, String endPoint, String region) {
        super(clientCredentials, endPoint, region);
    }

    @Override
    protected boolean initiateRequest() throws IOException {
        /* Send Header */
        System.out.println("Send CLEAR_CACHE Request");
        sout.write(RequestType.CLEAR_CACHE.getInt());
        sout.flush();

        /* Receive Success/Error Message */
        DataInputStream sin = new DataInputStream(socket.getInputStream());
        StatusProtocol status = new StatusProtocol(sin);
        if (status.getFailure()) {
            throw new IOException(status.getExceptionMessage());
        }

        return true;
    }

}
