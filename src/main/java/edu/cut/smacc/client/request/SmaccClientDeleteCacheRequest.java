package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.protocol.RequestType;

/**
 * Send a request to delete from cache to the SMACC server
 */
public class SmaccClientDeleteCacheRequest extends SmaccClientDeleteRequest {
    protected SmaccClientDeleteCacheRequest(BasicAWSCredentials clientCredentials, String endPoint, String region, String bucket, String key) {
        super(clientCredentials, endPoint, region, bucket, key);
        this.requestType = RequestType.DEL_CACHE;
    }
}