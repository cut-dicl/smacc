package edu.cut.smacc.server.cloud;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.StateType;
import edu.cut.smacc.server.tier.TierManager;

/**
 * An abstract class that exposes key interfaces for cloud store managers as
 * well as offering some common functionality for the different cloud store
 * managers.
 */
public abstract class CloudStoreManager {

    /* Key Interfaces to be implemented by cloud store managers */

    public abstract CloudFileWriter create(boolean async, String bucket, String key, Long length, CloudInfo cloudInfo)
            throws IOException;

    public abstract CloudFileReader read(String bucket, String key, CloudInfo cloudInfo)
            throws IOException;

    public abstract CloudFileReader read(String bucket, String key, long start, long stop, CloudInfo cloudInfo)
            throws IOException;

    public abstract CloudFile statFile(String bucket, String key, CloudInfo cloudInfo)
            throws IOException;

    public abstract List<CloudFile> list(String bucket, String prefix, CloudInfo cloudInfo)
            throws IOException;

    public abstract boolean delete(String bucket, String key, CloudInfo cloudInfo)
            throws IOException;

    /* Common functionalities between cloud store managers */

    private Map<String, HashMap<String, CloudFile>> cloudFiles; // bucket->key->file
    private Map<String, CloudInfo> bucketToCloudInfo; // bucket->CloudInfo

    private AsyncCloudUploadManager asynManager;
    private CloudInfoHandler credHandler;

    protected CloudStoreManager() {
        this.cloudFiles = Collections.synchronizedMap(new HashMap<>());
        this.bucketToCloudInfo = Collections.synchronizedMap(new HashMap<>());
    }

    public boolean initiate(TierManager tierMgr, String defaultBucket, CloudInfo defaultCloudInfo) {
        asynManager = new AsyncCloudUploadManager();
        asynManager.StartUploader(ServerConfigurations.getAsyncThreadPoolSize());

        credHandler = new CloudInfoHandler(bucketToCloudInfo);
        Thread credHandlerThread = new Thread(credHandler);
        credHandlerThread.start();

        return true;
    }

    public void shutdown() {
        asynManager.shutdownUploader();
        credHandler.shutdownHandler();

    }

    public boolean finalize(CloudFile cloudFile) {
        String bucket = cloudFile.getBucket();
        String key = cloudFile.getKey();

        synchronized (cloudFiles) { /* synchronize with delete */
            CloudFile saved_file = null;
            if (cloudFiles.containsKey(bucket)) {
                if (cloudFiles.get(bucket).containsKey(key)) {
                    saved_file = cloudFiles.get(bucket).get(key);
                    // We check if a newer file is already uploaded (in this case we come from the
                    // recovery and a live newer version is already uploaded)
                    if (saved_file.getVersion() > cloudFile.getVersion()) {
                        return false;
                    }
                }
            } else {
                cloudFiles.put(bucket, new HashMap<>());
            }

            // First check if it is NOT obsolete and then try to finalize (because of
            // non-block closing implementation)
            if (!cloudFile.isObsolete()) {
                if (saved_file != null) {
                    // Set previous file as obsolete
                    saved_file.setState(StateType.OBSOLETE);
                }
                cloudFiles.get(bucket).put(key, cloudFile);
                return true;
            }

            // Finalized failed or file obsolete [e.g., client disconnected]
            return false;
        }
    }

    protected boolean internalCloudFileExists(String bucket, String key) {
        return cloudFiles.containsKey(bucket) && cloudFiles.get(bucket).containsKey(key);
    }

    protected boolean deleteInternal(String bucket, String key) {
        boolean deleted = false;
        // Delete the internal minio file (metadata)
        synchronized (cloudFiles) {
            if (cloudFiles.containsKey(bucket) && cloudFiles.get(bucket).containsKey(key)) {
                cloudFiles.get(bucket).get(key).setState(StateType.OBSOLETE);
                cloudFiles.get(bucket).remove(key);
                deleted = true;
                if (cloudFiles.get(bucket).isEmpty())
                    cloudFiles.remove(bucket);
            }
        }

        return deleted;
    }

    public boolean saveBucketCloudInfo(String bucket, CloudInfo cloudInfo) {
        boolean newBucket = false;
        synchronized (bucketToCloudInfo) {
            if (!bucketToCloudInfo.containsKey(bucket)) {
                bucketToCloudInfo.put(bucket, cloudInfo);
                newBucket = true;
            }
        }
        return newBucket;
    }

    public CloudInfo getCloudInfoForBucket(String bucket) {
        synchronized (bucketToCloudInfo) {
            return bucketToCloudInfo.getOrDefault(bucket, null);
        }
    }

}
