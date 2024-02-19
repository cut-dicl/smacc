package edu.cut.smacc.server.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import edu.cut.smacc.server.cache.common.StateType;
import edu.cut.smacc.server.cache.common.io.*;
import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.cloud.CloudFileWriter;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.configuration.ServerConfigurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Used in order to write a file into the s3
 *
 * @author Theodoros Danos
 */
public class S3FileWriter extends CloudFileWriter {
    /* Static */
    private static final Logger logger = LogManager.getLogger(S3FileWriter.class);

    /* Instance */
    private CloudInfo cloudInfo;
    private CloudFile s3File;
    private String uploadId = null;
    private int currentPartId;
    private List<PartETag> partETags;
    private InitiateMultipartUploadResult initResponse;
    private OutputStream queueOut;
    private ByteBufferQueue internalQueue;
    private UsageStats queueStats;
    private int writeCount;
    private final int partSizeLimit = ServerConfigurations.getS3MultiPartSize(); // when buffer exceeds this limit the
                                                                                 // buffer will be flushed into s3 as a
                                                                                 // chunk (multi-part)
    private boolean streamClosed;
    private AmazonS3 s3client;
    private S3Manager s3mgr;
    private ByteBufferPool pool;
    private volatile int localParallelUploadCounter = 0;
    private S3ParallelUploadHandler s3ParallelHandler;
    private ObjectMetadata meta = null;
    private long totalBytesSent = 0;
    private String topicARN;
    private Date fileCreatedDate;
    private boolean nonBlockWaitForParallelUploads = true;
    private boolean uploadLastPartCheck = false;
    private boolean lastError = false;
    private boolean globalLockAcquired = false;

    private boolean lengthKnown;
    private static final ExecutorService executorService =
            Executors.newFixedThreadPool(ServerConfigurations.getClientHandlingThreadPoolSize());
    private Future<?> putThread;
    private WritableInputStream writableInputStream;

    private Object lock = new Object();

    S3FileWriter(String bucket, String key, InitiateMultipartUploadResult initR, AmazonS3 s3client, S3Manager s3mgr,
            ByteBufferPool pool, String topicARN, Date fileCreatedDate, CloudInfo cloudInfo) {
        UsageStats sts = new UsageStats();
        this.cloudInfo = cloudInfo;
        this.s3File = new CloudFile(bucket, key, 0);
        this.lengthKnown = false;
        this.pool = pool;
        this.partETags = new ArrayList<>();
        this.initResponse = initR;
        this.writeCount = 0;
        this.currentPartId = 1;
        this.queueStats = sts;
        this.internalQueue = new ByteBufferQueue(sts);
        this.queueOut = new ByteBufferQueueOutputStream(pool, internalQueue);
        this.streamClosed = false;
        this.s3client = s3client;
        this.s3mgr = s3mgr;
        if (initResponse != null) this.uploadId = initR.getUploadId();
        this.s3ParallelHandler = s3mgr.getS3ParallelUploadHandler();
        this.topicARN = topicARN;
        this.fileCreatedDate = fileCreatedDate;
        this.s3File.setLastModified(fileCreatedDate.getTime());
    }

    S3FileWriter(String bucket, String key, AmazonS3 s3client, S3Manager s3mgr, ByteBufferPool pool, String topicARN,
            Long length, CloudInfo cloudInfo) {
        UsageStats sts = new UsageStats();
        this.cloudInfo = cloudInfo;
        this.s3File = new CloudFile(bucket, key, length.longValue());
        this.lengthKnown = true;
        this.pool = pool;
        this.partETags = new ArrayList<>(1);
        this.writeCount = 0;
        this.currentPartId = 1;
        this.queueStats = sts;
        this.internalQueue = new ByteBufferQueue(sts);
        this.queueOut = new ByteBufferQueueOutputStream(pool, internalQueue);
        this.streamClosed = false;
        this.s3client = s3client;
        this.s3mgr = s3mgr;
        this.s3ParallelHandler = s3mgr.getS3ParallelUploadHandler();
        this.topicARN = topicARN;
        this.fileCreatedDate = new Date();
        this.s3File.setLastModified(fileCreatedDate.getTime());

        this.writableInputStream = new WritableInputStream(length);

        meta = new ObjectMetadata();
        meta.addUserMetadata("FileOwner", ServerConfigurations.getCacheSignature());
        meta.addUserMetadata("SMACC_DATE", String.valueOf(this.fileCreatedDate.getTime()));
        meta.setContentLength(length);

        putThread = executorService.submit(() -> {
            s3client.putObject(bucket, key, writableInputStream, meta);
            s3File.setState(StateType.COMPLETE);
            synchronized (lock) {
                lock.notify();
            }
        });
    }

    @Override
    public CloudFile getCloudFile() {
        return s3File;
    }

    AmazonS3 getS3Client() {
        if (s3client == null) {
            s3client = S3ClientPoolManager.getClient(s3File.getBucket(), cloudInfo);

            // Step 1: Initialize.
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(s3File.getBucket(),
                    s3File.getKey());
            //call s3 to get response with uploadID

            //set metadata
            fileCreatedDate = new Date();
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.addUserMetadata("FileOwner", ServerConfigurations.getCacheSignature());
            objectMetadata.addUserMetadata("SMACC_DATE", String.valueOf(fileCreatedDate.getTime()));
            initRequest.setObjectMetadata(objectMetadata);

            int retries = 0;
            while (retries <= ServerConfigurations.getS3MaxCreateRequests()) {
                retries += 1;
                try {
                    initResponse = s3client.initiateMultipartUpload(initRequest);
                    uploadId = initResponse.getUploadId();
                    break;
                } catch (SdkClientException e) {
                    System.err.println("Failed to initiate Multi-Upload request");
                    if (retries <= ServerConfigurations.getS3MaxCreateRequests())
                        logger.error("Fail to create multi-upload Create request");
                    else
                        setObsolete();
                }

            }

            // Save cloud info and add bucket event if needed
            if (s3mgr.saveBucketCloudInfo(s3File.getBucket(), cloudInfo)) {
                s3mgr.addBucketEvent(s3File.getBucket(), topicARN, s3client);
            }
        }

        return s3client;
    }

    public boolean isGoingManual() {
        if (writeCount > partSizeLimit) {
            boolean prevUploadSuccess = previousUploadsCheck(false);
            if (!prevUploadSuccess) lastError = true;

            if (localParallelUploadCounter < ServerConfigurations.getS3MaxParallelUploadLocal()) {
                if (s3ParallelHandler.acquireGlobalLock()) {
                    localParallelUploadCounter += 1;
                    globalLockAcquired = true;
                    return false;
                } else
                    return true;
            } else return true;

        } else return false;
    }

    public void abort() {
        if (!isObsolete()) {
            if (logger.isDebugEnabled()) logger.info("Client Disconnected - S3 Aborting Upload");
            cancelUploads();
            close();
        }
        streamClosed = true;
    }

    public void write(int b) throws IOException {
        if (isClosed() || isObsolete()) throw new IOException("Writing to a closed file");
        if (b >= 0)    //if not EOF
        {
            if (writeCount > partSizeLimit)
                uploadPart(false);    //its not the last because i have more to write

            queueOut.write(b);
            writeCount += 1;
            totalBytesSent += 1;

        } else close();
    }

    public void write(byte[] buff) throws IOException {
        if (isClosed() || isObsolete()) throw new IOException("Writing to a closed file");


        if (writeCount > partSizeLimit)
            uploadPart(false);    //its not the last because i have more to write

        queueOut.write(buff);
        writeCount += buff.length;
        totalBytesSent += buff.length;
    }

    public void write(byte[] buff, int offset, int len) throws IOException {
        if (isClosed() || isObsolete()) throw new IOException("Writing to a closed file");

        if (lengthKnown) {
            writableInputStream.write(buff, offset, len);
            return;
        }

        if (writeCount > partSizeLimit)
            uploadPart(false);    //its not the last because i have more to write

        queueOut.write(buff, offset, len);
        writeCount += len;
        totalBytesSent += len;
    }

    public long getTotalBytesSent() {
        return totalBytesSent;
    }

    public void flush() {
    }    //do nothing

    @Override
    public void close() {
        if (!isClosed() && !isObsolete()) {
            logger.debug("closing s3 file ...");
            streamClosed = true;
            writableInputStream.close();
            S3ClientPoolManager.releaseClient(s3File.getBucket(), s3client);
        }
    }

    public void uploadLastPart() throws IOException {
        /* Check is necessary in case user reconnect and same method is called twice */
        if (!uploadLastPartCheck) {
            uploadLastPartCheck = true;
            uploadPart(true);
        }
    }

    public boolean isUploadingMultiPart() {
        if (!nonBlockWaitForParallelUploads)
            return false;
        else if (!s3ParallelHandler.isUploading(uploadId, localParallelUploadCounter)) {
            if (nonBlockWaitForParallelUploads)
                previousUploadsCheck(true);    //check if previous uploads succeeded
            else
                return false;
        } else
            return true;

        return s3ParallelHandler.isUploading(uploadId, localParallelUploadCounter);
    }

    private boolean manualUploadPart(boolean lastPart) {

        if (logger.isDebugEnabled())
            logger.info("Manual Flushing - key:" + s3File.getKey() + " PartID:" + currentPartId);

        int retried = 0;

        while (retried < ServerConfigurations.getS3MaxUploadRetries()) {
            retried += 1;
            try {
                //upload to s3
                getS3Client();    //connect to s3 if not connected

                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(s3File.getBucket()).withKey(s3File.getKey())
                        .withUploadId(uploadId).withPartNumber(currentPartId).withPartSize(writeCount);
                uploadRequest.getRequestClientOptions().setReadLimit(writeCount);

                if (lastPart) {
                    logger.debug("This is the last Part");
                    uploadRequest.setLastPart(true);
                }

                uploadRequest.setInputStream(new ByteBufferQueueInputStream(internalQueue));

                // Upload part and add response to our list.
                partETags.add(getS3Client().uploadPart(uploadRequest).getPartETag());

                if (logger.isDebugEnabled())
                    logger.info("Manual Flushing COMPLETE - key:" + s3File.getKey() + " PartID:" + currentPartId);
                break;
            } catch (SdkClientException e) {
                logger.error("PART UPLOAD FAILED:" + e.getMessage());
                try {
                    Thread.sleep(ServerConfigurations.getS3MaxUploadRetryDelay());
                } catch (InterruptedException ignored) {
                }
            } catch (Exception e) {
                logger.error("UNEXPECTED ERROR IN MULTI-UPLOAD:" + e.getMessage(), e);
                break;
            }
        }
        internalQueue.delete(pool);
        return retried < ServerConfigurations.getS3MaxUploadRetries();
    }

    private void cancelUploads() {
        logger.info("CANCELING UPLOAD");
        if (uploadId != null) {
            s3ParallelHandler.cancelUpload(uploadId);
            s3ParallelHandler.waitParallelUploadThreads(uploadId, localParallelUploadCounter);    //we wait the threads to finish uploading
            s3ParallelHandler.deleteAllParallelUploadFutures(uploadId);
            abortUpload();    //deallocate the parts stored on s3
        }
        setObsolete();
        localParallelUploadCounter = 0;
    }

    private boolean previousUploadsCheck(boolean cancelOnFailure) {
        PartETag tag;
        if (uploadId == null) getS3Client();
        List<Future<PartETag>> parallelUploadFutures = s3ParallelHandler.getParallelUploadFutures(uploadId);

        if (parallelUploadFutures != null) { //because this method is called to the beginning of the uploadPart method, where no  threads are created yet (or finished)
            Iterator<Future<PartETag>> iter = parallelUploadFutures.iterator();
            while (iter.hasNext()) {
                Future<PartETag> future = iter.next();
                iter.remove();
                localParallelUploadCounter -= 1;
                if (future.isDone()) {
                    tag = null;
                    try {
                        tag = future.get();
                    } catch (Exception ignored) {
                    }

                    if (tag != null) { //upload succeeded
                        partETags.add(tag);
                    } else { //upload failed
                        if (cancelOnFailure) {
                            cancelUploads();
                        }
                        return false;
                    }
                }
            }
        } else return true;

        return true;
    }

    private void uploadPart(boolean lastPart) throws IOException {
        boolean manualUploadSuccess = true;
        boolean prevUploadSuccess = !lastError;
        nonBlockWaitForParallelUploads = false;
        if (uploadId == null) getS3Client();
        if (!isClosed()) {
            if (writeCount > 0) {
                if (!lastPart && writeCount <= partSizeLimit)
                    throw new IOException("Trying buffer flushing but buffer not full yet + is not last part");
                queueOut.close();    //close stream of the internalQueue. Its time to flush queue to s3

                //check the status of possible finished uploads to see if any has failed
                if (!lastError) prevUploadSuccess = previousUploadsCheck(false);
                if (prevUploadSuccess) {
                    //upload the buffer later (auto) or now (manually) based on local and global counter
                    if (lastPart || globalLockAcquired || localParallelUploadCounter < ServerConfigurations.getS3MaxParallelUploadLocal()) {
                        if (!globalLockAcquired) {
                            if (!s3ParallelHandler.tryParallelUpload(this, lastPart, internalQueue, writeCount, currentPartId)) {
                                manualUploadSuccess = manualUploadPart(lastPart);

                            } else localParallelUploadCounter += 1;
                        } else {
                            s3ParallelHandler.setParallelUpload(this, lastPart, internalQueue, writeCount, currentPartId);
                            globalLockAcquired = false;
                        }
                    } else
                        manualUploadSuccess = manualUploadPart(false);

                    currentPartId += 1;
                    writeCount = 0;

                    if (!lastPart && manualUploadSuccess) {
                        queueStats = new UsageStats();
                        internalQueue = new ByteBufferQueue(queueStats);
                        queueOut = new ByteBufferQueueOutputStream(pool, internalQueue);

                    } else if (manualUploadSuccess) {
                        if (logger.isDebugEnabled())
                            logger.info("All buffers flushed and waiting for upload process to finish");
                        if (localParallelUploadCounter > 0) {
                            //Wait for threads to finish..
                            nonBlockWaitForParallelUploads = true;
                        }
                        return;
                    }
                }

                if (!prevUploadSuccess || !manualUploadSuccess) {
                    logger.info("Multi-Part Upload Fail - Waiting for threads to finish");
                    internalQueue.delete(pool);
                    if (previousUploadsCheck(true)) cancelUploads(); //in case no other threads left to check
                    lastError = false;
                }
            }
        }
    }

    @Override
    public boolean completeFile() {
        if (s3File.isIncomplete()) {
            if (finalizeUpload() && s3mgr.finalize(s3File)) {
                //create state file
                s3File.setState(StateType.COMPLETE); // complete
                if (logger.isDebugEnabled())
                    logger.info("S3 UPLOAD COMPLETE:" + s3File.getKey() + " V:[" + s3File.getVersion() + "]");
                return true;
            } else {
                setObsolete();    //obsolete
                if (uploadId != null) {
                    abortUpload();    //abort uploading
                }
                return false;
            }

        } else {
            return s3File.isComplete();
        }
    }

    boolean finalizeUpload() {
        final int MaxRetries = 3;
        int retries = 0;

        while (retries < MaxRetries) {
            retries += 1;
            try {

                ObjectMetadata s3CurrentFileMeta;
                Date s3CurrentFileDate = null;
                Date smaccDate = null;

                try {
                    s3CurrentFileMeta = getS3Client().getObjectMetadata(s3File.getBucket(), s3File.getKey());
                    s3CurrentFileDate = s3CurrentFileMeta.getLastModified();
                    if (s3CurrentFileMeta.getUserMetadata().containsKey("SMACC_DATE"))
                        smaccDate = new Date(Long.parseLong(s3CurrentFileMeta.getUserMetaDataOf("SMACC_DATE")));
                } catch (SdkClientException e) { /* Do nothing - Metadata not available */ }


                if (s3CurrentFileDate == null || checkS3Date(fileCreatedDate, s3CurrentFileDate, smaccDate)) {
                    CompleteMultipartUploadRequest compRequest =
                            new CompleteMultipartUploadRequest(s3File.getBucket(), s3File.getKey(), uploadId,
                                    partETags);
                    if (!lengthKnown) {
                        getS3Client().completeMultipartUpload(compRequest);
                    }

                    meta = getS3Client().getObjectMetadata(s3File.getBucket(), s3File.getKey());
                    s3File.setLength(meta.getContentLength());

                    if (logger.isDebugEnabled())
                        logger.info("Upload Success..." + s3File.getKey() + " v[" + s3File.getVersion() + "]");
                } else {
                    logger.info("Upload failed - S3 has a newer file - we are late");
                    return false;
                }

                break;
            } catch (Exception e) {
                logger.info("Retrying.. Cannot Finalize Upload:" + e.getMessage(), e);
            }
        }
        return retries != MaxRetries;
    }

    private boolean checkS3Date(Date fileDate, Date s3Date, Date smaccDate) {
        if (smaccDate == null)
            return s3Date.before(fileDate);
        else
            return smaccDate.before(fileDate);
    }

    private void abortUpload() {
        try {
            logger.info("Abort Uploading... " + s3File.getKey() + " V:" + s3File.getVersion());

            getS3Client().abortMultipartUpload(new AbortMultipartUploadRequest(
                    s3File.getBucket(), s3File.getKey(), uploadId));

        } catch (SdkClientException e) {
            logger.info("Fail to delete chunks left on s3 - Client Communication Error");
        }
    }

    String getUploadId() {
        if (uploadId == null) getS3Client();
        return uploadId;
    }

    public String getKey() {
        return s3File.getKey();
    }

    public long getVersion() {
        return s3File.getVersion();
    }

    public String getBucket() {
        return s3File.getBucket();
    }

    private boolean isClosed() {
        return streamClosed;
    }

    public boolean isObsolete() {
        return s3File.isObsolete();
    }

    public boolean isUploadingWithLength() {
        return !putThread.isDone();
    }

    public void waitUploadWithLength(int timeout) throws InterruptedException {
        synchronized (lock) {
            lock.wait(timeout);
        }
    }

    void setObsolete() {
        if (!isObsolete()) {
            if (logger.isDebugEnabled())
                logger.info("S3 File Obsolete " + s3File.getKey() + " V:" + s3File.getVersion());
            s3File.setState(StateType.OBSOLETE);
            partETags.clear();
        }
    }
}