package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.cache.common.BlockRange;

/**
 * A factory for creating SMACC client requests
 */
public abstract class SmaccClientRequestFactory {

    private static BasicAWSCredentials credentials;
    private static String endPoint;
    private static String region;

    /**
     * Initialize the factory with the credentials the server endpoint, and the region
     * @param credentials the credentials
     * @param endPoint the server endpoint
     * @param region the region
     */
    public static void init(BasicAWSCredentials credentials, String endPoint, String region) {
        SmaccClientRequestFactory.credentials = credentials;
        SmaccClientRequestFactory.endPoint = endPoint;
        SmaccClientRequestFactory.region = region;
    }

    /**
     * Create a request to get SMACC Server statistics
     * @return the request
     */
    public static SmaccClientRequest createStatsRequest() {
        checkInitialization();
        return new SmaccClientStatisticsRequest(credentials, endPoint, region);
    }

    /**
     * Create a request to reset the SMACC Server statistics
     * @return the request
     */
    public static SmaccClientRequest createResetStatsRequest() {
        checkInitialization();
        return new SmaccClientResetStatisticsRequest(credentials, endPoint, region);
    }

    /**
     * Create a request to shut down the SMACC server
     * @return the request
     */
    public static SmaccClientRequest createShutdownRequest() {
        checkInitialization();
        return new SmaccClientShutdownRequest(credentials, endPoint, region);
    }

    /**
     * Create a request to clear the SMACC cache
     * @return the request
     */
    public static SmaccClientRequest createClearCacheRequest() {
        checkInitialization();
        return new SmaccClientClearCacheRequest(credentials, endPoint, region);
    }

    /**
     * Create a request to delete an object or a directory of objects from the SMACC / S3
     * @param bucket the bucket
     * @param key the file key or the directory prefix
     * @return the request
     */
    public static SmaccClientRequest createDeleteRequest(String bucket, String key) {
        checkInitialization();
        return new SmaccClientDeleteRequest(credentials, endPoint, region, bucket, key);
    }

    /**
     * Create a request to delete an object or a directory of objects from the SMACC cache only
     * @param bucket the bucket
     * @param key the file key or the directory prefix
     * @return the request
     */
    public static SmaccClientRequest createDeleteCacheRequest(String bucket, String key) {
        checkInitialization();
        return new SmaccClientDeleteCacheRequest(credentials, endPoint, region, bucket, key);
    }

    /**
     * Create a request to list objects from the SMACC server
     * @param bucket the bucket
     * @param prefix the directory prefix (null for the whole bucket)
     * @return the request
     */
    public static SmaccClientRequest createListCacheRequest(String bucket, String prefix) {
        checkInitialization();
        return new SmaccClientListCacheRequest(credentials, endPoint, region, bucket, prefix);
    }

    /**
     * Create a request to get the status of an object from the SMACC server
     * @param bucket the bucket
     * @param key the file key
     * @return the request
     */
    public static SmaccClientRequest createFileStatusRequest(String bucket, String key) {
        checkInitialization();
        return new SmaccClientFileStatusRequest(credentials, endPoint, region, bucket, key);
    }

    /**
     * Create a request to get an object or a directory of objects from the SMACC server
     * @param bucket the bucket
     * @param key the file key or the directory prefix
     * @param range the block range (null for the whole object)
     * @return the request
     */
    public static SmaccClientRequest createGetRequest(String bucket, String key, BlockRange range) {
        checkInitialization();
        return new SmaccClientInputStream(bucket, key, range, credentials, endPoint, region);
    }

    /**
     * Create a request to put an object or a directory of objects to the SMACC server
     * @param bucket the bucket
     * @param key the file key or the directory prefix
     * @param async whether the request should be asynchronous
     * @param length the length of the object
     * @return the request
     */
    public static SmaccClientRequest createPutRequest(String bucket, String key, boolean async, Long length) {
        checkInitialization();
        return new SmaccClientOutputStream(bucket, key, async, credentials, endPoint, region, length);
    }

    /**
     * Check if the factory was initialized
     */
    private static void checkInitialization() {
        if (credentials == null || endPoint == null || region == null) {
            throw new IllegalStateException("Factory not initialized. Call init() first.");
        }
    }

}
