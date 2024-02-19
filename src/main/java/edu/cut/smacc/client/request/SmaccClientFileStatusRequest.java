package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.cache.common.SMACCObject;
import edu.cut.smacc.server.protocol.RequestType;

import java.io.DataInputStream;
import java.io.IOException;

/**
 *  Client request to get the status of a file in the SMACC cache or S3
 */
public class SmaccClientFileStatusRequest extends SmaccClientBucketKeyRequest {

    private SMACCObject smaccObject;

    protected SmaccClientFileStatusRequest(BasicAWSCredentials clientCredentials, String endPoint, String region, String bucket, String key) {
        super(clientCredentials, endPoint, region, bucket, key, RequestType.FILE_STATUS);
    }

    @Override
    protected boolean initiateRequest() throws IOException {
        DataInputStream sin = new DataInputStream(socket.getInputStream());
        System.out.println("Send FILE_STATUS Request for " + key);
        writeHeader();

        boolean fileExists = sin.readBoolean(); // Check if file exists
        if (!fileExists) {
            return false;
        }

        smaccObject = SMACCObject.receive(sin);
        return true;
    }

    public SMACCObject getSmaccObject() {
        return smaccObject;
    }
}
