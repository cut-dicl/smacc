package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.protocol.RequestType;

import java.io.IOException;

public abstract class SmaccClientBucketKeyRequest extends SmaccClientAbstractRequest {

    protected final String key;
    protected final String bucket;
    protected RequestType requestType;

    protected SmaccClientBucketKeyRequest(BasicAWSCredentials clientCredentials, String endPoint, String region,
            String bucket, String key, RequestType requestType) {
        super(clientCredentials, endPoint, region);
        this.bucket = bucket;
        this.key = key;
        this.requestType = requestType;
    }

    protected void writeHeader() throws IOException {
        sout.write(requestType.getInt());
        sout.write(bucket.length());
        sout.write(bucket.getBytes());
        if (key == null) {
            sout.write(0);
        } else {
            sout.write(key.length());
            sout.write(key.getBytes());
        }
    }

}
