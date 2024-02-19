package edu.cut.smacc.server.s3;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cloud.CloudInfo;

/**
 * Maintains a pool of s3Client instances that can be reused. There are two main
 * methods involved: getS3Client for getting an s3Client (new or reused), and
 * releaseS3Client for returning the s3Client back to the pool so that it can be
 * reused.
 */
public class S3ClientPoolManager {

    private static ConcurrentHashMap<String, ConcurrentLinkedQueue<AmazonS3>> s3ClientsMap = new ConcurrentHashMap<>();

    /**
     * Gets an instance of s3Client. This method will either return an existing one
     * or create a new one (if none is available). When someone gets an s3Client,
     * no-one else can use it.
     * 
     * @param bucket
     * @param cloudInfo
     * @return
     */
    public static AmazonS3 getClient(String bucket, CloudInfo cloudInfo) {

        ConcurrentLinkedQueue<AmazonS3> s3ClientsQueue = s3ClientsMap.get(bucket);

        AmazonS3 s3Client = null;
        if (s3ClientsQueue != null) {
            s3Client = s3ClientsQueue.poll();
        }

        if (s3Client == null) {
            s3Client = createS3Client(cloudInfo);
        }

        return s3Client;
    }

    /**
     * Returns the s3Client back into the pool so that others can use it.
     * 
     * @param bucket
     * @param s3Client
     */
    public static void releaseClient(String bucket, AmazonS3 s3Client) {
        synchronized (s3ClientsMap) {
            if (!s3ClientsMap.containsKey(bucket)) {
                s3ClientsMap.put(bucket, new ConcurrentLinkedQueue<>());
            }
        }

        ConcurrentLinkedQueue<AmazonS3> s3ClientsQueue = s3ClientsMap.get(bucket);
        s3ClientsQueue.offer(s3Client);
    }

    /**
     * Creates a new s3Client instance
     */
    private static AmazonS3 createS3Client(CloudInfo cloudInfo) {
        ClientConfiguration s3ClientConfigs = new ClientConfiguration();
        s3ClientConfigs.setConnectionTimeout(ServerConfigurations.getS3ConnectionTimeout());
        s3ClientConfigs.setSocketTimeout(ServerConfigurations.getS3SocketnTimeout());

        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(cloudInfo.getAccessKey(), cloudInfo.getSecretKey()));
        EndpointConfiguration endPointConfiguration = new EndpointConfiguration(cloudInfo.getEndPoint(),
                cloudInfo.getRegion());

        return AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(endPointConfiguration)
                .withClientConfiguration(s3ClientConfigs).build();
    }
}
