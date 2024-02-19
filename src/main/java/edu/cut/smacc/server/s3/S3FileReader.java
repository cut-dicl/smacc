package edu.cut.smacc.server.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.cloud.CloudFileReader;
import edu.cut.smacc.server.cloud.CloudInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * used in order to read an object from s3
 *
 * @author Theodoros Danos
 */
public class S3FileReader extends CloudFileReader {
    /* Static */
    private static final Logger logger = LogManager.getLogger(S3FileReader.class);

    private CloudFile s3File;

    private AmazonS3 s3Client;
    private S3ObjectInputStream internalInput = null;
    private ObjectMetadata meta;
    private boolean s3ObjectInRange = true;

    S3FileReader(String bucket, String key, AmazonS3 s3Client, S3Manager s3mgr, String topicARN, CloudInfo cloudInfo)
            throws IOException {
        this.s3Client = s3Client;

        try {
            S3Object s3object = s3Client.getObject(bucket, key);
            internalInput = s3object.getObjectContent();
            meta = s3object.getObjectMetadata();
            s3File = new CloudFile(bucket, key, meta.getContentLength(), meta.getLastModified().getTime(),
                    isOwnedFile());

        } catch (Exception e) {
            this.close();
            logger.debug("Unable to read s3 file: " + e.getMessage());
            throw new IOException(e);
        }
    }

    S3FileReader(String bucket, String key, AmazonS3 s3Client, long start, long stop, S3Manager s3mgr, String topicARN,
            CloudInfo cloudInfo) throws IOException {
        this.s3Client = s3Client;

        try {
            meta = s3Client.getObjectMetadata(bucket, key);
            if (meta.getContentLength() < stop) {
                s3ObjectInRange = false;
                return;
            }

            s3File = new CloudFile(bucket, key, meta.getContentLength(), meta.getLastModified().getTime(),
                    isOwnedFile());
            GetObjectRequest objReq = new GetObjectRequest(bucket, key);
            objReq.setRange(start, stop);
            S3Object s3object = s3Client.getObject(objReq);
            meta = s3object.getObjectMetadata();
            internalInput = s3object.getObjectContent();

        } catch (Exception e) {
            this.close();
            logger.debug("S3 file Range-Reader:");
            throw new IOException(e);
        }
    }

    @Override
    public CloudFile getCloudFile() {
        return s3File;
    }

    public boolean isOwnedFile() {
        Map<String, String> metaMap = meta.getUserMetadata();

        if (metaMap.containsKey("FileOwner")) {
            return meta.getUserMetaDataOf("FileOwner").equals(ServerConfigurations.getCacheSignature());
        } else
            return false;
    }

    boolean isInRange() {
        return s3ObjectInRange;
    }

    public int read() throws IOException {
        return internalInput.read();
    }

    public int read(byte[] buff) throws IOException {
        return internalInput.read(buff);
    }

    public int read(byte[] buff, int off, int len) throws IOException {
        return internalInput.read(buff, off, len);
    }

    public int available() throws IOException {
        throw new IOException("Do not use available(). aws sdk's bug");
    }

    @Override
    public void close() throws IOException {
        if (internalInput != null) {
            internalInput.close();
        }
        if (s3Client != null) {
            S3ClientPoolManager.releaseClient(s3File.getBucket(), s3Client);
            s3Client = null;
        }
    }

}