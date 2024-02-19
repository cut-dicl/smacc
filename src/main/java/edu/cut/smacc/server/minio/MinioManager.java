package edu.cut.smacc.server.minio;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.cloud.CloudStoreManager;
import edu.cut.smacc.server.tier.TierManager;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Item;

/**
 * A cloud store manager for storing/accessing data from MinIO
 */
public class MinioManager extends CloudStoreManager {

    public MinioManager() {
        super();
    }

    public boolean initiate(TierManager tierMgr, String defaultBucket, CloudInfo defaultCloudInfo) {
        super.initiate(tierMgr, defaultBucket, defaultCloudInfo);

        MinioClientPoolManager.releaseClient(defaultBucket,
                MinioClientPoolManager.getClient(defaultBucket, defaultCloudInfo));

        return true;
    }

    @Override
    public MinioFileWriter create(boolean async, String bucket, String key, Long length, CloudInfo cloudInfo)
            throws IOException {
        if (length == null) {
            throw new IOException("MinioManager current does not support uploading without length. Object "
                    + bucket + ":" + key);
        }
        saveBucketCloudInfo(bucket, cloudInfo);

        MinioClient minioClient = MinioClientPoolManager.getClient(bucket, cloudInfo);
        return new MinioFileWriter(this, minioClient, bucket, key, length.longValue());
    }

    @Override
    public MinioFileReader read(String bucket, String key, CloudInfo cloudInfo) throws IOException {

        CloudFile minioFile = statFile(bucket, key, cloudInfo);
        if (minioFile != null) {
            saveBucketCloudInfo(bucket, cloudInfo);

            MinioClient minioClient = MinioClientPoolManager.getClient(bucket, cloudInfo);
            return new MinioFileReader(this, minioClient, minioFile);
        } else {
            return null;
        }
    }

    @Override
    public MinioFileReader read(String bucket, String key, long start, long stop,
            CloudInfo cloudInfo) throws IOException {

        CloudFile minioFile = statFile(bucket, key, cloudInfo);
        if (minioFile != null) {
            saveBucketCloudInfo(bucket, cloudInfo);

            MinioClient minioClient = MinioClientPoolManager.getClient(bucket, cloudInfo);
            return new MinioFileReader(this, minioClient, minioFile, start, stop);
        } else {
            return null;
        }
    }

    @Override
    public CloudFile statFile(String bucket, String key, CloudInfo cloudInfo) throws IOException {
        // Get object information and metadata
        MinioClient minioClient = MinioClientPoolManager.getClient(bucket, cloudInfo);
        StatObjectResponse objectStat = null;
        try {
            objectStat = minioClient.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build());
        } catch (ErrorResponseException e) {
            // File does not exist
            return null;
        } catch (Exception e) {
            // Some other exception happened
            throw new IOException("Error reading MinIO file", e);
        } finally {
            MinioClientPoolManager.releaseClient(bucket, minioClient);
        }

        // Create the corresponding minioFile
        long lastModified = objectStat.lastModified().toInstant().toEpochMilli();
        Map<String, String> metadata = objectStat.userMetadata();
        boolean isOwnedFile = metadata != null && metadata.containsKey("fileowner")
                && metadata.get("fileowner").equals(ServerConfigurations.getCacheSignature());

        return new CloudFile(bucket, key, objectStat.size(), lastModified, isOwnedFile);
    }

    @Override
    public List<CloudFile> list(String bucket, String prefix, CloudInfo cloudInfo) throws IOException {

        MinioClient minioClient = MinioClientPoolManager.getClient(bucket, cloudInfo);

        Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder()
                .bucket(bucket)
                .prefix(prefix)
                .includeUserMetadata(true)
                .build());

        List<CloudFile> minioFiles = new ArrayList<>();
        for (Result<Item> result : results) {
            try {
                Item item = result.get();
                if (item.isDir())
                    continue;

                long lastModified = item.lastModified().toInstant().toEpochMilli();
                Map<String, String> metadata = item.userMetadata();
                boolean isOwnedFile = metadata != null && metadata.containsKey("X-Amz-Meta-Fileowner")
                        && metadata.get("X-Amz-Meta-Fileowner").equals(ServerConfigurations.getCacheSignature());

                minioFiles.add(new CloudFile(bucket, item.objectName(), item.size(), lastModified, isOwnedFile));
            } catch (Exception e) {
                MinioClientPoolManager.releaseClient(bucket, minioClient);
                throw new IOException("Failed to list objects in " + bucket, e);
            }
        }

        MinioClientPoolManager.releaseClient(bucket, minioClient);

        return minioFiles;
    }

    @Override
    public boolean delete(String bucket, String key, CloudInfo cloudInfo) throws IOException {

        // Delete the internal minio file (metadata)
        deleteInternal(bucket, key);

        // Check if file exists
        CloudFile minioFile = statFile(bucket, key, cloudInfo);
        if (minioFile != null) {
            // Delete the file
            MinioClient minioClient = MinioClientPoolManager.getClient(bucket, cloudInfo);
            try {
                minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucket).object(key).build());
                return true;
            } catch (Exception e) {
                throw new IOException("Failed to delete file " + minioFile, e);
            } finally {
                MinioClientPoolManager.releaseClient(bucket, minioClient);
            }
        } else {
            // File does not exist
            return false;
        }
    }

}
