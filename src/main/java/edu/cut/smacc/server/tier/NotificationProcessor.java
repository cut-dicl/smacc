package edu.cut.smacc.server.tier;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.s3.S3ClientPoolManager;
import edu.cut.smacc.server.tier.NotificationEvent.EventType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.Map;

/**
 * This is a thread running each time there is a notification message that affects smacc's files
 * The class is processing the notification and acts accordingly
 *
 * @author Theodoros Danos
 */
public class NotificationProcessor implements Runnable {
    private static final Logger logger = LogManager.getLogger(NotificationProcessor.class);

    private TierManager tier;
    private NotificationEvent event;

    NotificationProcessor(TierManager tier, NotificationEvent event) {
        this.tier = tier;
        this.event = event;
    }

    public void run() {
        byte[] buffer;
        String bucket = event.getBucket();
        String key = event.getKey();

        CloudInfo cloudInfo = tier.getCloudInfoForBucket(bucket); // credentials is null if we don't have the bucket

        if (event.getEventType() == EventType.DELETED && !tier.isPending(bucket, key)) {
            if (logger.isDebugEnabled()) logger.info("SNS UPDATE: DELETE FILE " + key);
            tier.deleteFileFromCache(bucket, key);
        } else if (cloudInfo != null && event.getEventType() == EventType.CREATED) {
            // Wait until there is no upload in progress. This is because, if there is an
            // upload and a download in same time, an older object may evict a newer file
            // from cache (closing after the newer file is closed)
            while (tier.isPending(bucket, key)) {
                Thread.yield();
                try {
                    Thread.sleep(ServerConfigurations.getWaitPendingFilesTimeMs());
                } catch (InterruptedException ignored) {
                }
            }

            AmazonS3 s3Client = S3ClientPoolManager.getClient(bucket, cloudInfo);
            ObjectMetadata meta;
            try {
                meta = s3Client.getObjectMetadata(bucket, key);
            } catch (SdkClientException e) {
                logger.error("SNS Processing Thread: Retrieve Metadata from s3 failed: " + e.getMessage());
                return;
            }

            Map<String, String> metadata = meta.getUserMetadata();

            if (!metadata.containsKey("FileOwner") || !metadata.get("FileOwner").equals(ServerConfigurations.getCacheSignature())) {
                if (logger.isDebugEnabled()) logger.info("SNS UPDATE: UPDATING FILE USING s3 " + key);

                InputStream in;
                try {
                    in = tier.dumpRead(bucket, key, cloudInfo);
                    if (in != null) { // In case of null, it means we are not caching
                        buffer = new byte[ServerConfigurations.getDownloadNewFileBufferSize()];
                        while (in.read(buffer) > 0);
                        in.close();

                    } else {
                        tier.deleteFileFromCache(bucket, key);    //if i don't want to fetch new data, old one must be deleted
                    }
                } catch (Exception e) {
                    logger.error("Downloading s3 file (SNS Update) failed ", e);
                }
            }
        }
    }
}
