package edu.cut.smacc.server.main.request;

import edu.cut.smacc.server.cache.common.SMACCObject;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.protocol.HeaderServer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Request handler for SMACC LIST_CACHE requests.
 */
public class ListCacheRequestHandler extends RequestHandlerBase {

    protected ListCacheRequestHandler(ClientConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    public void handleRequest(HeaderServer header, CloudInfo cloudInfo) {
        String prefix = header.hasListPrefix() ? header.getListPrefix() : null;
        String bucket = header.getBucket();

        List<SMACCObject> list = tier.list(bucket, prefix);
        DataOutputStream cout = connectionHandler.getDataOutputStream();

        try { // Send
            cout.writeShort(list.size());
            for (SMACCObject object : list) {
                object.send(cout);
            }
            cout.flush();
            connectionHandler.closeConnection();
        } catch (IOException e) {
            connectionHandler.closeConnection();
        }
    }
}
