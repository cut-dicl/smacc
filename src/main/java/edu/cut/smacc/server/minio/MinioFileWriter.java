package edu.cut.smacc.server.minio;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.StateType;
import edu.cut.smacc.server.cache.common.io.WritableInputStream;
import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.cloud.CloudFileWriter;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;

/**
 * An output stream writer for writing a file/object into MinIO
 * 
 */
public class MinioFileWriter extends CloudFileWriter {

    private static final Logger logger = LogManager.getLogger(MinioFileWriter.class);

    public static final ExecutorService executorService = Executors
            .newFixedThreadPool(ServerConfigurations.getClientHandlingThreadPoolSize());

    private MinioManager minioMgr;
    private MinioClient minioClient;
    private CloudFile minioFile;

    private boolean isClosed;
    private WritableInputStream writableInputStream;

    private final byte[] singleByte = new byte[1];
    private final Object lock = new Object();
    private final Future<?> putThread;

    /**
     * 
     * @param minioMgr
     * @param minioClient
     * @param bucket
     * @param key
     * @param length
     */
    public MinioFileWriter(MinioManager minioMgr, MinioClient minioClient, String bucket, String key, long length) {
        this.minioMgr = minioMgr;
        this.minioClient = minioClient;
        this.minioFile = new CloudFile(bucket, key, length);

        this.isClosed = false;
        this.writableInputStream = new WritableInputStream(length);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("fileowner", ServerConfigurations.getCacheSignature());
        metadata.put("SMACC_DATE", String.valueOf(minioFile.getLastModified()));

        putThread = executorService.submit(() -> {
            try {
                minioClient.putObject(PutObjectArgs.builder().bucket(bucket).object(key)
                        .stream(writableInputStream, length, -1).userMetadata(metadata).build());
            } catch (Exception e) {
                logger.error("Error uploading object " + bucket + ":" + key, e);
            }

            synchronized (lock) {
                lock.notify();
            }
        });
    }

    @Override
    public CloudFile getCloudFile() {
        return minioFile;
    }

    @Override
    public void write(int b) throws IOException {
        singleByte[0] = (byte) b;
        write(singleByte, 0, 1);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte[] buff, int offset, int len) throws IOException {
        if (isClosed || minioFile.isObsolete())
            throw new IOException("Writing to a closed file");

        writableInputStream.write(buff, offset, len);
    }

    @Override
    public void close() {
        if (!isClosed && !minioFile.isObsolete()) {
            if (logger.isDebugEnabled())
                logger.debug("Closing minio file " + minioFile);
            isClosed = true;
            writableInputStream.close();
            MinioClientPoolManager.releaseClient(minioFile.getBucket(), minioClient);
        }
    }

    @Override
    public boolean completeFile() {
        if (minioFile.isIncomplete()) {
            if (minioMgr.finalize(minioFile)) {
                minioFile.setState(StateType.COMPLETE);
                if (logger.isDebugEnabled())
                    logger.debug("Completed uploading minio file " + minioFile);
                return true;
            } else {
                minioFile.setState(StateType.OBSOLETE);
                return false;
            }
        } else {
            return minioFile.isComplete();
        }
    }

    @Override
    public boolean isUploadingMultiPart() {
        return false;
    }

    @Override
    public boolean isUploadingWithLength() {
        return !putThread.isDone();
    }

    @Override
    public void waitUploadWithLength(int timeout) throws InterruptedException {
        synchronized (lock) {
            lock.wait(timeout);
        }
    }

    @Override
    public void abort() {
        // nothing to do
    }

    @Override
    public boolean isGoingManual() {
        return false;
    }

    @Override
    public void uploadLastPart() throws IOException {
        // nothing to do
    }

}
