package edu.cut.smacc.server.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import edu.cut.smacc.server.cache.common.io.ByteBufferPool;
import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.cloud.CloudStoreManager;
import edu.cut.smacc.server.tier.NotificationHandler;
import edu.cut.smacc.server.tier.TierManager;
import edu.cut.smacc.configuration.ServerConfigurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Controls the S3FileWriter objects which correspond to each s3 connection / request
 *
 * @author everest
 */
public class S3Manager extends CloudStoreManager {
    /* Static */
    private static final Logger logger = LogManager.getLogger(S3Manager.class);

    /* Instance */
    private ByteBufferPool pool;
    private S3ParallelUploadHandler s3ParallelHandler;
    private NotificationHandler snsHandler = null;

    public S3Manager() {
        super();
        this.pool = new ByteBufferPool(ServerConfigurations.getS3ByteBufferSize());
    }

    public S3FileWriter create(boolean async, String bucket, String key, Long length, CloudInfo cloudInfo)
            throws IOException {
        if (length != null) {
            AmazonS3 s3Client = S3ClientPoolManager.getClient(bucket, cloudInfo);
            // Save cloud info and add bucket event if needed
            if (saveBucketCloudInfo(bucket, cloudInfo)) {
                addBucketEvent(bucket, getSNSTopicARN(), s3Client);
            }

            return new S3FileWriter(bucket, key, s3Client, this, pool, getSNSTopicARN(), length, cloudInfo);
        }
        InitiateMultipartUploadResult initResponse = null;
        AmazonS3 s3Client = null;
        Date createdDate = new Date();
        if (!async) {
            s3Client = S3ClientPoolManager.getClient(bucket, cloudInfo);

            // Step 1: Initialize.
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key);
            //call s3 to get response with uploadID

            //set metadata
            createdDate = new Date();
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.addUserMetadata("FileOwner", ServerConfigurations.getCacheSignature());
            objectMetadata.addUserMetadata("SMACC_DATE", String.valueOf(createdDate.getTime()));
            initRequest.setObjectMetadata(objectMetadata);

            int retries = 0;
            while (retries <= ServerConfigurations.getS3MaxCreateRequests()) {
                retries += 1;
                try {
                    initResponse = s3Client.initiateMultipartUpload(initRequest);
                    break;
                } catch (SdkClientException e) {
                    System.err.println("Failed to initiate Multi-Upload request");
                    if (retries <= ServerConfigurations.getS3MaxCreateRequests())
                        logger.error("Fail to create multi-upload Create request");
                    else throw new IOException(e);
                }

            }
        }

        // Save cloud info and add bucket event if needed
        if (saveBucketCloudInfo(bucket, cloudInfo)) {
            addBucketEvent(bucket, getSNSTopicARN(), s3Client);
        }

        return new S3FileWriter(bucket, key, initResponse, s3Client, this, pool, getSNSTopicARN(), createdDate,
                cloudInfo);
    }

    public S3FileReader read(String bucket, String key, CloudInfo cloudInfo) throws IOException {

        AmazonS3 s3Client = S3ClientPoolManager.getClient(bucket, cloudInfo);

        try {
            if (s3Client.doesObjectExist(bucket, key)) {
                // Save cloud info and add bucket event if needed
                if (saveBucketCloudInfo(bucket, cloudInfo)) {
                    addBucketEvent(bucket, getSNSTopicARN(), s3Client);
                }

                return new S3FileReader(bucket, key, s3Client, this, getSNSTopicARN(), cloudInfo);
            }
            return null;
        } catch (SdkClientException e) {
            throw new IOException(e);
        }
    }

    public S3FileReader read(String bucket, String key, long start, long stop, CloudInfo cloudInfo) throws IOException {
        AmazonS3 s3Client = S3ClientPoolManager.getClient(bucket, cloudInfo);

        try {
            if (s3Client.doesObjectExist(bucket, key)) {
                // Save cloud info and add bucket event if needed
                if (saveBucketCloudInfo(bucket, cloudInfo)) {
                    addBucketEvent(bucket, getSNSTopicARN(), s3Client);
                }

                S3FileReader reader = new S3FileReader(bucket, key, s3Client, start, stop, this, getSNSTopicARN(),
                        cloudInfo);
                if (reader.isInRange()) {
                    return reader;
                }
                return null;
            }
            return null;
        } catch (SdkClientException e) {
            throw new IOException(e);
        }
    }

    public boolean delete(String bucket, String key, CloudInfo cloudInfo) {
        AmazonS3 s3Client = S3ClientPoolManager.getClient(bucket, cloudInfo);
        try {
            deleteInternal(bucket, key);

            if (s3Client.doesObjectExist(bucket, key)) {
                s3Client.deleteObject(bucket, key);
                return true;
            }
            return false;
        } catch (SdkClientException e) {
            return false;
        } finally {
            S3ClientPoolManager.releaseClient(bucket, s3Client);
        }
    }

    void addBucketEvent(String bucket, String arn, AmazonS3 s3Client) {
        if (arn == null)
            return; // SNS notification is disabled

        /* Add Bucket Event Notification */
        try {
            if (logger.isDebugEnabled()) logger.info("Adding Bucket Event on bucket: " + bucket);

            BucketNotificationConfiguration notificationConfiguration = new BucketNotificationConfiguration();
            notificationConfiguration.addConfiguration("snsTopicConfig",
                    new TopicConfiguration(arn, EnumSet.of(S3Event.ObjectCreated, S3Event.ObjectRemoved)));
            SetBucketNotificationConfigurationRequest request =
                    new SetBucketNotificationConfigurationRequest(bucket, notificationConfiguration);
            s3Client.setBucketNotificationConfiguration(request);
        } catch (SdkClientException e) {
            logger.error("Could not add sns event to bucket: " + e.getMessage());
        }
    }

    /**
     * Used during recovery.
     *
     * @param bucket
     * @param key
     * @return Object Metadata
     */
    public ObjectMetadata getObjectMetadata(String bucket, String key, CloudInfo cloudInfo) {
        AmazonS3 s3Client = S3ClientPoolManager.getClient(bucket, cloudInfo);
        try {
            return s3Client.getObjectMetadata(bucket, key);
        } catch (Exception e) {
            return null;
        } finally {
            S3ClientPoolManager.releaseClient(bucket, s3Client);
        }
    }

    S3ParallelUploadHandler getS3ParallelUploadHandler() {
        return s3ParallelHandler;
    }

    public void shutdown() {
        super.shutdown();
        s3ParallelHandler.shutdownHandler();
        if (snsHandler != null)
            snsHandler.shutdown();
    }

    private String getSNSTopicARN() {
        return (snsHandler == null) ? null : snsHandler.getSNSTopicARN();
    }

    public boolean initiate(TierManager tierMgr, String defaultBucket, CloudInfo defaultCloudInfo) {
        super.initiate(tierMgr, defaultBucket, defaultCloudInfo);

        s3ParallelHandler = new S3ParallelUploadHandler(pool);
        s3ParallelHandler.startHandler(ServerConfigurations.getParallelUploadHandlerThreadPoolSize());

        // Initiate SNS Service
        if (ServerConfigurations.getSNSNotificationActivate()) {
            this.snsHandler = new NotificationHandler(tierMgr);
            Thread snsThread = new Thread(snsHandler);
            snsThread.start();
        }

        S3ClientPoolManager.releaseClient(defaultBucket,
                S3ClientPoolManager.getClient(defaultBucket, defaultCloudInfo));

        return true;
    }

    @Override
    public CloudFile statFile(String bucket, String key, CloudInfo cloudInfo) throws IOException {

        CloudFile cloudFile = null;
        AmazonS3 s3Client = S3ClientPoolManager.getClient(bucket, cloudInfo);
        try {
            ObjectMetadata meta = s3Client.getObjectMetadata(bucket, key);

            Map<String, String> metadata = meta.getUserMetadata();
            boolean isOwner = metadata.containsKey("FileOwner")
                    && metadata.get("FileOwner").equals(ServerConfigurations.getCacheSignature());

            cloudFile = new CloudFile(bucket, key, meta.getContentLength(), meta.getLastModified().getTime(), isOwner);
        } catch (Exception e) {
            return null;
        } finally {
            S3ClientPoolManager.releaseClient(bucket, s3Client);
        }

        return cloudFile;
    }

    @Override
    public List<CloudFile> list(String bucket, String prefix, CloudInfo cloudInfo) throws IOException {
        AmazonS3 s3Client = S3ClientPoolManager.getClient(bucket, cloudInfo);

        ObjectListing listing = s3Client.listObjects(bucket, prefix);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        // getting the list of files in the bucket
        while (listing.isTruncated()) {
            listing = s3Client.listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
        }

        List<CloudFile> cloudFiles = new ArrayList<>();
        for (S3ObjectSummary summary : summaries) {
            String key = summary.getKey();
            cloudFiles.add(new CloudFile(bucket, key, summary.getSize(),
                    summary.getLastModified().getTime(), internalCloudFileExists(bucket, key)));
        }

        return cloudFiles;
    }
}

