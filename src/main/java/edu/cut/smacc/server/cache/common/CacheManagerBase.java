package edu.cut.smacc.server.cache.common;


import edu.cut.smacc.server.cache.disk.DiskFile;
import edu.cut.smacc.server.cache.disk.DiskManager;
import edu.cut.smacc.server.cache.memory.MemoryFile;
import edu.cut.smacc.server.cache.memory.MemoryManager;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnCacheOperation;
import edu.cut.smacc.server.statistics.type.general.TierGeneralStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An abstract class to handle the common operations of the cache managers
 */
public abstract class CacheManagerBase implements CacheManager {

    private final Logger logger = LogManager.getLogger(CacheManagerBase.class);

    protected final Map<String, HashMap<String, CacheFile>> cacheMapping;
    private final TierGeneralStatistics tierGeneralStatistics;
    protected final StatisticsUpdaterOnCacheOperation statsUpdater;

    protected CachePolicyNotifier policyNotifier;

    private long cacheBytes; // used for statistics


    protected CacheManagerBase() {
        this.cacheMapping = Collections.synchronizedMap(new HashMap<>());
        this.tierGeneralStatistics = new TierGeneralStatistics();
        this.statsUpdater = new StatisticsUpdaterOnCacheOperation(tierGeneralStatistics, this);
        this.tierGeneralStatistics.setParentUpdater(statsUpdater);
        this.cacheBytes = 0;
    }

    public void setPolicyNotifier(CachePolicyNotifier notifier) {
        this.policyNotifier = notifier;
    }

    @Override
    public CacheFile create(String bucket, String key, long actualSize) throws IOException {
        CacheFile file = this.create(bucket, key);
        file.setActualSize(actualSize);
        file.setPartialFile();
        return file;
    }

    @Override
    public CacheFile getFile(String bucket, String key) {
        synchronized (cacheMapping) {
            if (containsObject(bucket, key) && this.cacheMapping.get(bucket).containsKey(key)) {
                return this.cacheMapping.get(bucket).get(key);
            }
            return null;
        }
    }

    @Override
    public boolean containsObject(String bucket, String key) {
        synchronized (cacheMapping) {
            if (!this.cacheMapping.containsKey(bucket))
                return false;
            return cacheMapping.get(bucket).containsKey(key);
        }
    }

    public Set<CacheFile> list(String bucket) {
        synchronized (cacheMapping) {
            Set<CacheFile> list = new HashSet<>();
            if (cacheMapping.containsKey(bucket)) {
                list.addAll(cacheMapping.get(bucket).values());
            }
            return list;
        }
    }

    @Override
    public boolean delete(String bucket, String key) {
        synchronized (cacheMapping) {
            if (cacheMapping.containsKey(bucket) && cacheMapping.get(bucket).containsKey(key)) {
                CacheFile file = cacheMapping.get(bucket).get(key);
                policyNotifier.notifyItemDeletion(file, getStoreOptionType());
                file.delete();
                cacheMapping.get(bucket).remove(key);
                cacheBytes -= file.getTotalSize();
                if (cacheMapping.get(bucket).isEmpty()) {
                    cacheMapping.remove(bucket);
                }
            }
            return true;
        }
    }

    public boolean evict(CacheFile file) {
        String bucket = file.getBucket();
        String key = file.getKey();
        synchronized (cacheMapping) {
            if (cacheMapping.containsKey(bucket) && cacheMapping.get(bucket).containsKey(key)) {
                CacheFile cfile = cacheMapping.get(bucket).get(key);
                cacheMapping.get(bucket).remove(key);
                cacheBytes -= cfile.getTotalSize();
                policyNotifier.notifyItemDeletion(cfile, getStoreOptionType());
                if (cacheMapping.get(bucket).isEmpty()) {
                    cacheMapping.remove(bucket);
                }
                return true;
            }
        }
        return false;
    }

    public void put(String bucket, String key, CacheFile file) {
        synchronized (cacheMapping) {
            CacheFile prevf;
            if (cacheMapping.containsKey(bucket)) {
                if (cacheMapping.get(bucket).containsKey(key)) {
                    prevf = cacheMapping.get(bucket).get(key);
                    if (prevf.getVersion() > file.getVersion())    //you are late
                    {
                        if (logger.isDebugEnabled()) {
                            if (file instanceof MemoryFile) {
                                logger.info("MEMORY: Delete file on put - Late put - V:" + file.getVersion());
                            } else if (file instanceof DiskFile) {
                                logger.info("DISK: Delete file on put - Late put - V:" + file.getVersion());
                            }
                            cacheBytes -= file.getTotalSize();
                            policyNotifier.notifyItemDeletion(file, getStoreOptionType());
                            file.delete();
                            return;
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            if (this instanceof MemoryManager) {
                                logger.info("[MEMORY] Attempt to delete previous file - NEW AVAILABLE(" + file.getVersion() + ")");
                            } else if (this instanceof DiskManager) {
                                logger.info("[DISK] Attempt to delete previous file - NEW AVAILABLE(" + file.getVersion() + ")");
                            }
                        }
                        cacheBytes -= prevf.getTotalSize();
                        policyNotifier.notifyItemDeletion(prevf, getStoreOptionType());
                        prevf.delete();
                    }
                }
            } else {
                cacheMapping.put(bucket, new HashMap<>());
            }
            cacheMapping.get(bucket).put(key, file);
            cacheBytes += file.getTotalSize();
            policyNotifier.notifyItemAddition(file, getStoreOptionType());
        }
    }
    
    public void checkVersion(String bucket, String key, long NewVersion) {
        CacheFile tempFile = null;
        synchronized (cacheMapping) {
            if (cacheMapping.containsKey(bucket) && cacheMapping.get(bucket).containsKey(key))
                tempFile = cacheMapping.get(bucket).get(key);
            
            if (tempFile != null) {
                long current_version = tempFile.getVersion();
                if (current_version < NewVersion) {
                    // Delete the file
                    if (logger.isDebugEnabled()) {
                        if (this instanceof DiskManager) {
                            logger.info("DISK-VERSION OUTDATED - DELETING VER " + current_version);
                        } else if (this instanceof MemoryManager) {
                            logger.info("MEM-VERSION OUTDATED - DELETING VER " + current_version);
                        }
                    }
                    if (cacheMapping.get(bucket).get(key) == tempFile) {
                        cacheMapping.get(bucket).remove(key);
                        cacheBytes -= tempFile.getTotalSize();
                    }
                    tempFile.delete();
                }
            }
        }
    }

    protected boolean fileDoesNotExist(String bucket, String key) {
        return !this.cacheMapping.containsKey(bucket) || !this.cacheMapping.get(bucket).containsKey(key);
    }

    protected boolean fileIsPartial(String bucket, String key) {
        return cacheMapping.get(bucket).get(key).isPartialFile() ||
                !cacheMapping.get(bucket).get(key).isFullFile();
    }

    public TierGeneralStatistics getTierStatistics() {
        return tierGeneralStatistics;
    }

    @Override
    public long getCacheBytes() {
        return cacheBytes;
    }

    abstract protected long getReportedUsage();

}
