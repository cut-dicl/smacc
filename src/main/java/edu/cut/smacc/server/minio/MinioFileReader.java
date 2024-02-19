package edu.cut.smacc.server.minio;

import java.io.IOException;
import java.io.InputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.cloud.CloudFileReader;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;

/**
 * An input stream reader for reading a file/object from MinIO
 */
public class MinioFileReader extends CloudFileReader {
    private static final Logger logger = LogManager.getLogger(MinioFileReader.class);

    private MinioClient minioClient;
    private CloudFile minioFile;

    private InputStream inputStream;

    public MinioFileReader(MinioManager minioMgr, MinioClient minioClient, CloudFile minioFile) throws IOException {
        this(minioMgr, minioClient, minioFile, 0, minioFile.getLength() - 1);
    }

    public MinioFileReader(MinioManager minioMgr, MinioClient minioClient, CloudFile minioFile, long start,
            long stop) throws IOException {
        this.minioClient = minioClient;
        this.minioFile = minioFile;

        if (start < 0 || stop >= minioFile.getLength())
            throw new IOException("Invlide read range for minio file " + minioFile);

        long length = stop - start + 1; // both start & stop are inclusive

        try {
            inputStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(minioFile.getBucket())
                            .object(minioFile.getKey())
                            .offset(start)
                            .length(length)
                            .build());
        } catch (Exception e) {
            close();
            logger.error("Unable to read minio file " + minioFile, e);
            throw new IOException("Unable to read minio file " + minioFile, e);
        }
    }

    @Override
    public CloudFile getCloudFile() {
        return minioFile;
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public int read(byte[] buff) throws IOException {
        return inputStream.read(buff);
    }

    @Override
    public int read(byte[] buff, int off, int len) throws IOException {
        return inputStream.read(buff, off, len);
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
            inputStream = null;
            if (logger.isDebugEnabled())
                logger.debug("Completed reading minio file " + minioFile);
        }
        if (minioClient != null) {
            MinioClientPoolManager.releaseClient(minioFile.getBucket(), minioClient);
            minioClient = null;
        }
    }

    public boolean isOwnedFile() {
        return minioFile.isOwnedFile();
    }

}
