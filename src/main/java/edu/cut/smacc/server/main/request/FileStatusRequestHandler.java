package edu.cut.smacc.server.main.request;

import edu.cut.smacc.server.cache.common.SMACCObject;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.protocol.HeaderServer;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Request handler for SMACC FILE_STATUS requests.
 */
public class FileStatusRequestHandler extends RequestHandlerBase {

    protected FileStatusRequestHandler(ClientConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    public void handleRequest(HeaderServer header, CloudInfo cloudInfo) {
        String bucket = header.getBucket();
        String key = header.getKey();
        DataOutputStream cout = connectionHandler.getDataOutputStream();

        SMACCObject object = tier.getSMACCObject(bucket, key);
        try {
            if (object == null) {
                cout.writeBoolean(false);
            } else {
                cout.writeBoolean(true);
                object.send(cout);
            }
            cout.flush();
            connectionHandler.closeConnection();
        } catch (IOException e) {
            connectionHandler.closeConnection();
        }
    }
}
