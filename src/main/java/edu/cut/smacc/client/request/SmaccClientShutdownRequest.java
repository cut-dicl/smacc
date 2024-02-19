package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.protocol.RequestType;

import java.io.IOException;

public class SmaccClientShutdownRequest extends SmaccClientAbstractRequest {

    SmaccClientShutdownRequest(BasicAWSCredentials clientCredentials, String endPoint, String region) {
        super(clientCredentials, endPoint, region);
    }

    @Override
    protected boolean initiateRequest() throws IOException {
        /* Send Header */
        System.out.println("Send SHUTDOWN Request");
        sout.write(RequestType.SHUTDOWN.getInt());
        sout.flush();

        return true;
    }

}
