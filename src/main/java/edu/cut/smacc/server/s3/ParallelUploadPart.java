package edu.cut.smacc.server.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import edu.cut.smacc.server.cache.common.io.ByteBufferPool;
import edu.cut.smacc.server.cache.common.io.ByteBufferQueue;
import edu.cut.smacc.server.cache.common.io.ByteBufferQueueInputStream;
import edu.cut.smacc.configuration.ServerConfigurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * a thread uploading a buffer of a multi-part upload operation, to s3
 *
 * @author Theodoros Danos
 */
public class ParallelUploadPart implements Callable<PartETag> {
    /* Static */
    private static final Logger logger = LogManager.getLogger(ParallelUploadPart.class);

    /* Instance */
    private boolean lastPart;
    private ByteBufferQueue queue;
    private String uploadId;
    private int currentPartId;
    private String bucket;
    private String key;
    private AmazonS3 s3Client;
    private int partSize;
    private ByteBufferPool pool;

    ParallelUploadPart(boolean lastPart, ByteBufferQueue queue, String uploadId, int currentPartId, String bucket, String key, AmazonS3 s3Client, int writeCount, ByteBufferPool pool) {
        this.lastPart = lastPart;
        this.queue = queue;
        this.uploadId = uploadId;
        this.currentPartId = currentPartId;
        this.bucket = bucket;
        this.key = key;
        this.s3Client = s3Client;
        this.partSize = writeCount;
        this.pool = pool;
    }

    void releaseMemory() {
        queue.delete(pool);
    }

    String getUploadId() {
        return uploadId;
    }

    public PartETag call() {
        PartETag retTag = null;
        if (logger.isDebugEnabled()) logger.info("Auto Flushing - key:" + key + " PartID:" + currentPartId);

        int retried = 0;

        while (retried < ServerConfigurations.getS3MaxUploadRetries()) {
            retried += 1;
            try {
                //upload to s3

                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucket).withKey(key)
                        .withUploadId(uploadId).withPartNumber(currentPartId).withPartSize(partSize);
                uploadRequest.getRequestClientOptions().setReadLimit(partSize);

                if (lastPart) {
                    if (logger.isDebugEnabled()) logger.info("This is the last Part :PID:" + currentPartId);
                    uploadRequest.setLastPart(true);
                }

                uploadRequest.setInputStream(new ByteBufferQueueInputStream(queue));

                // Upload part and add response to our list.
                retTag = s3Client.uploadPart(uploadRequest).getPartETag();
                if (logger.isDebugEnabled())
                    logger.info("Auto Flushing COMPLETE - key:" + key + " PartID:" + currentPartId);
                break;
            } catch (SdkClientException e) {
                logger.info("PART UPLOAD FAILED:" + e.getMessage());
                retTag = null;
                try {
                    Thread.sleep(ServerConfigurations.getS3MaxUploadRetryDelay());
                } catch (InterruptedException ignored) {
                }
            } catch (Exception e) {
                logger.error("UNEXPECTED ERROR IN MULTI-UPLOAD:" + e.getMessage());
                e.printStackTrace();
                retTag = null;
            }
        }

        if (retried >= ServerConfigurations.getS3MaxUploadRetries())
            retTag = null;

        return retTag;
    }
}

