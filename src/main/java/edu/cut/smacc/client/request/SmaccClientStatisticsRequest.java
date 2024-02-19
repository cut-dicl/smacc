package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.protocol.RequestType;

import java.io.IOException;

public class SmaccClientStatisticsRequest extends SmaccClientAbstractRequest {

    SmaccClientStatisticsRequest(BasicAWSCredentials clientCredentials, String endPoint, String region) {
        super(clientCredentials, endPoint, region);
    }

    @Override
    protected boolean initiateRequest() throws IOException {
        /* Send Header */
        System.out.println("Send COLLECT_STATS Request");
        sout.write(RequestType.COLLECT_STATS.getInt());
        sout.flush();

        return true;
    }

}
