package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.protocol.RequestType;

import java.io.IOException;

/**
 * Client request to reset the SMACC Server statistics
 */
public class SmaccClientResetStatisticsRequest extends SmaccClientAbstractRequest {

    protected SmaccClientResetStatisticsRequest(BasicAWSCredentials clientCredentials, String endPoint, String region) {
        super(clientCredentials, endPoint, region);
    }

    @Override
    protected boolean initiateRequest() throws IOException {
        /* Send Header */
        System.out.println("Send RESET_STATS Request");
        sout.write(RequestType.RESET_STATS.getInt());
        sout.flush();

        return true;
    }
}
