package edu.cut.smacc.server.cache.memory;


import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.CacheManagerBase;
import edu.cut.smacc.server.cache.common.FilenameFeatureExtractor;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.server.cache.common.io.ByteBufferPool;
import edu.cut.smacc.server.cache.common.io.UsageStats;

import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Memory manager handles the memory files
 *
 * @author everest
 */
public class MemoryManager extends CacheManagerBase {
    private static final Logger logger = LogManager.getLogger(MemoryManager.class);

    /* *********** INSTANCE **************** */
    private final ByteBufferPool pool;
    private final StoreSettings settings;
    private final UsageStats memStats;
    private final boolean isActive;

    public MemoryManager(StoreSettings settings, CachePolicyNotifier policyNotifier) {
        setPolicyNotifier(policyNotifier);
        this.pool = new ByteBufferPool(ServerConfigurations.getMemoryByteBufferSize());
        this.settings = settings;
        this.memStats = (settings != null) ? settings.getStats() : new UsageStats();
        this.isActive = settings != null && settings.getStats().getMaxCapacity() > 0;
    }

    @Override
    public boolean isActive() {
        return isActive;
    }

    @Override
    public ArrayList<CacheFile> getCacheFiles() {
        ArrayList<CacheFile> files = new ArrayList<>((int) getCacheFilesCount());
        synchronized (cacheMapping) {
            for (String bucket : cacheMapping.keySet())
                for (String key : cacheMapping.get(bucket).keySet())
                    files.add(cacheMapping.get(bucket).get(key));
        }
        return files;
    }

    @Override
    public long getCacheFilesCount() {
        long count = 0;
        synchronized (cacheMapping) {
            for (String bucket : cacheMapping.keySet())
                count += cacheMapping.get(bucket).size();
        }
        return count;
    }

    @Override
    public MemoryFile create(String bucket, String key) {
        touchBucket(bucket); //create bucket record if not exist

        return new MemoryFile(this.pool, bucket, key, settings, this, memStats);
    }

    @Override
    public InputStream read(String bucket, String key) throws IOException {
        synchronized (this.cacheMapping) {
            if (!checkFileExist(bucket, key)) {
                return null;
            }
            CacheFile cacheFile = this.cacheMapping.get(bucket).get(key);
            policyNotifier.notifyItemAccess(cacheFile, getStoreOptionType());
            return cacheFile.getInputStream();
        }
    }

    @Override
    public InputStream read(String bucket, String key, long start, long stop) throws IOException {
        synchronized (this.cacheMapping) {
            try {
                if (fileDoesNotExist(bucket, key) || fileIsPartial(bucket, key)) {
                    return null;
                }
                CacheFile cacheFile = this.cacheMapping.get(bucket).get(key);
                policyNotifier.notifyItemAccess(cacheFile, getStoreOptionType());
                return ((MemoryFile) cacheFile).getInputStream(start, stop);
            } catch (FileNotFoundException e) {
                logger.error(e);
                return null;
            }
        }
    }

    @Override
    public StoreOptionType getStoreOptionType() {
        return StoreOptionType.MEMORY_ONLY;
    }

    private void touchBucket(String bucket) {
        if (!cacheMapping.containsKey(bucket)) {
            cacheMapping.put(bucket, new HashMap<>());
        }
    }

    public long getReportedUsage() {
        return memStats.getReportedUsage();
    }

    public HashMap<String, ArrayList<FilenameFeatureExtractor>> initiateRecovery() {
        HashMap<String, ArrayList<FilenameFeatureExtractor>> returnList = new HashMap<>();

        //	Memory Recovery
        try {
            File statefolder = new File(settings.getStateFolder());
            File[] statelistOfFiles = statefolder.listFiles();
            if (statelistOfFiles == null) {
                return null;
            }
            //	Recover complete files (obsolete and pushed files are not going to be recovered)
            for (File statelistOfFile : statelistOfFiles) {
                if (logger.isDebugEnabled()) logger.info("Check File[M] " + statelistOfFile.getName());
                if (statelistOfFile.isFile()) {
                    if (statelistOfFile.getName().contains("##")) {
                        String stateFilename = statelistOfFile.getName();
                        FilenameFeatureExtractor features = new FilenameFeatureExtractor(stateFilename);
                        if (features.isCacheFile()) {

                            //	Delete file if obsolete
                            if (features.isObsolete() || features.isToBePushed()) {
                                if (logger.isDebugEnabled())
                                    logger.info("Delete Obsolete DiskFile " + statelistOfFile.getName());
                                if (statelistOfFile.delete()) {
                                    logger.info("DiskFile deleted successfully");
                                } else {
                                    logger.info("Failed to delete the DiskFile");
                                }
                                continue;
                            }

                            //	if file is in complete state, make the file visible
                            if (features.isComplete()) {
                                String bucketKey = features.getBucket() + features.getKey();
                                if (!returnList.containsKey(bucketKey)) {
                                    returnList.put(bucketKey, new ArrayList<>());
                                    returnList.get(bucketKey).add(features);
                                } else
                                    returnList.get(bucketKey).add(features);
                            }
                        }

                    }
                }
            }

            return returnList;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    private void clearMemory() {
        this.pool.clear();
    }

    public void shutdown() {
        clearMemory();
    }

    private boolean checkFileExist(String bucket, String key) {
        return cacheMapping.containsKey(bucket) && cacheMapping.get(bucket).containsKey(key);
    }
}