package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.cache.common.SMACCObject;
import edu.cut.smacc.server.protocol.RequestType;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Client request to list all files in the SMACC cache
 */
public class SmaccClientListCacheRequest extends SmaccClientBucketKeyRequest {

    private final List<SMACCObject> smaccObjects;

    protected SmaccClientListCacheRequest(BasicAWSCredentials clientCredentials, String endPoint, String region,
                                          String bucket, String prefix) {
        super(clientCredentials, endPoint, region, bucket, prefix, RequestType.LIST_CACHE);
        this.smaccObjects = new ArrayList<>();
    }

    @Override
    protected boolean initiateRequest() throws IOException {
        DataInputStream sin = new DataInputStream(socket.getInputStream());
        System.out.println("Send LIST Request for " + (key == null ? "all" : key));
        writeHeader();

        short size = sin.readShort(); // Receive list
        for (int i = 0; i < size; i++) {
            smaccObjects.add(SMACCObject.receive(sin));
        }
        return true;
    }

    public List<SMACCObject> getSmaccObjects() {
        return smaccObjects;
    }

}
