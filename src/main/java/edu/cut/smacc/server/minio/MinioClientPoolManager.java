package edu.cut.smacc.server.minio;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import edu.cut.smacc.server.cloud.CloudInfo;
import io.minio.MinioClient;

/**
 * Maintains a pool of minioClient instances that can be reused. There are two
 * main methods involved: getClient for getting an minioClient (new or reused),
 * and releaseClient for returning the minioClient back to the pool so that it
 * can be reused.
 */
public class MinioClientPoolManager {

    // bucket -> queue of clients
    private static ConcurrentHashMap<String, ConcurrentLinkedQueue<MinioClient>> clientsMap = new ConcurrentHashMap<>();

    /**
     * Gets an instance of MinioClient. This method will either return an existing
     * one or create a new one (if none is available). When someone gets an
     * MinioClient, no-one else can use it.
     * 
     * @param bucket
     * @param endPoint
     * @param region
     * @param accessKey
     * @param secretKey
     * @return
     */
    public static MinioClient getClient(String bucket, CloudInfo cloudInfo) {

        ConcurrentLinkedQueue<MinioClient> clientsQueue = clientsMap.get(bucket);

        MinioClient minioClient = null;
        if (clientsQueue != null) {
            minioClient = clientsQueue.poll();
        }

        String region = cloudInfo.getRegion();
        if (region == null || region.equals(""))
            region = null;

        if (minioClient == null) {
            minioClient = MinioClient.builder()
                    .endpoint(cloudInfo.getEndPoint())
                    .region(region)
                    .credentials(cloudInfo.getAccessKey(), cloudInfo.getSecretKey())
                    .build();
        }

        return minioClient;
    }

    /**
     * Returns the MinioClient back into the pool so that others can use it.
     * 
     * @param bucket
     * @param minioClient
     */
    public static void releaseClient(String bucket, MinioClient minioClient) {
        synchronized (clientsMap) {
            if (!clientsMap.containsKey(bucket)) {
                clientsMap.put(bucket, new ConcurrentLinkedQueue<>());
            }
        }

        ConcurrentLinkedQueue<MinioClient> clientsQueue = clientsMap.get(bucket);
        clientsQueue.offer(minioClient);
    }

}
