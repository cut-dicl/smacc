package edu.cut.smacc.server.tier;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.CacheFileHelper;
import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.common.io.InputStreamCacheSplitter;
import edu.cut.smacc.server.cache.disk.DiskFile;
import edu.cut.smacc.server.cache.disk.DiskManager;
import edu.cut.smacc.server.cache.memory.MemoryFile;
import edu.cut.smacc.server.cache.memory.MemoryManager;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import edu.cut.smacc.server.cache.policy.admission.AdmissionPolicy;
import edu.cut.smacc.configuration.ConfigurationException;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import edu.cut.smacc.server.cache.policy.eviction.placement.EvictionPlacementPolicy;
import edu.cut.smacc.server.cache.policy.eviction.trigger.EvictionTriggerPolicy;
import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.cloud.CloudFileReader;
import edu.cut.smacc.server.cloud.CloudFileWriter;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.cloud.CloudStoreManager;
import edu.cut.smacc.server.minio.MinioManager;
import edu.cut.smacc.server.s3.S3Manager;
import edu.cut.smacc.server.statistics.type.Statistics;
import edu.cut.smacc.server.statistics.type.performance.PerformanceStatistics;
import edu.cut.smacc.server.statistics.type.general.TierGeneralStatistics;
import edu.cut.smacc.server.statistics.updater.StatisticsTimeUpdaterOnOperation;
import edu.cut.smacc.server.tier.result.DeleteResult;
import edu.cut.smacc.server.tier.result.GetResult;
import edu.cut.smacc.server.tier.result.PutResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This tier manager handles all the managers in smacc such as memory manager,
 * disk manager and s3 manager
 * It is responsible to organize the cache system, to initiate the recovery and
 * to clean up in shutdown
 * This class is the abstract form of the smacc system
 *
 * @author Theodoros Danos
 * @author Michail Boronikolas
 * @author Herodotos Herodotou
 */
public class TierManager {
    /* ************ STATIC *************** */
    private static final Logger logger = LogManager.getLogger(TierManager.class);
    private final static Map<String, Map<String, AtomicLong>> versionCounter = Collections
            .synchronizedMap(new HashMap<>());


    static long getVersion(String bucket, String key) {
        long returnVersion;

        synchronized (versionCounter) {
            if (!versionCounter.containsKey(bucket))
                versionCounter.put(bucket, Collections.synchronizedMap(new HashMap<>()));

            Map<String, AtomicLong> innerVersionCounter = versionCounter.get(bucket);
            if (innerVersionCounter.containsKey(key)) {
                // get version for bucket and key
                returnVersion = innerVersionCounter.get(key).getAndIncrement();
            } else {
                innerVersionCounter.put(key, new AtomicLong(2L));
                returnVersion = 1;
            }
        }

        return returnVersion;
    }

    private static void setCurrentVersion(String bucket, String key, long version) { // Used in recovery
        synchronized (versionCounter) {
            if (!versionCounter.containsKey(bucket))
                versionCounter.put(bucket, Collections.synchronizedMap(new HashMap<>()));
            versionCounter.get(bucket).put(key, new AtomicLong(version + 1));
        }
    }

    private static void removeVersion(String bucket, String key) {
        synchronized (versionCounter) {
            if (versionCounter.containsKey(bucket))
                versionCounter.get(bucket).remove(key);
        }
    }

    /* *********** INSTANCE ************* */

    private MemoryManager memMgr;
    private DiskManager dmgr;
    private CloudStoreManager cloudMgr;
    private Statistics storageStatistics;
    private HashMap<String, HashMap<String, AtomicInteger>> pendingUploads;
    private AdmissionPolicy admissionPolicy;
    private EvictionTriggerPolicy evictionTriggerPolicy;

    private CachePolicyNotifier policyNotifier;

    private EvictionManager evictionManager;
    private ExecutorService downgrationHandler;
    private volatile boolean recoveryDone = false;

    public TierManager(Configuration configuration) {
        dmgr = null;
        cloudMgr = null;
        memMgr = null;

        initiate(configuration);            //Initiate Managers (Recover)
    }

    /* Interface */

    public PutResult create(String bucket, String key, boolean async, Long length,
            CloudInfo cloudInfo)
            throws IOException {
        if (!dmgr.isActive() && !memMgr.isActive() && async)
            throw new IOException("Cache disk and memory not configured - cannot apply async ops");

        PutResult result = new PutResult();
        CacheOutputStream finalOutput;

        try {
            // Check if the file exists in the cache (memory or disk)
            MemoryFile existingMemoryFile = null;
            DiskFile existingDiskFile = null;
            if (memMgr.isActive() && memMgr.containsObject(bucket, key)) {
                existingMemoryFile = (MemoryFile) memMgr.getFile(bucket, key);
            }
            if (dmgr.isActive() && dmgr.containsObject(bucket, key)) {
                existingDiskFile = (DiskFile) dmgr.getFile(bucket, key);
            }

            addPending(bucket, key);
            // Out to S3
            CloudFileWriter outs3 = cloudMgr.create(async, bucket, key, length, cloudInfo);
            CacheFile s3CacheFile = CacheFileHelper.createS3CacheFile(outs3.getCloudFile());
            StoreOptionType selectedStoreOption = admissionPolicy.getWriteAdmissionLocation(s3CacheFile);
            selectedStoreOption = ensureValidStoreOptionType(selectedStoreOption);
            switch (selectedStoreOption) {
                case MEMORY_ONLY -> {    //Memory  + s3
					if (existingMemoryFile != null) {
						policyNotifier.notifyItemUpdate(existingMemoryFile, StoreOptionType.MEMORY_ONLY);
					}
					//if (existingDiskFile != null) {
						CacheFileHelper.handleDelete(bucket, key, dmgr);
					//}

					if (logger.isDebugEnabled())
						logger.info("CREATE: MEM ONLY");
                    CacheFile cFile = CacheFileHelper.createTierCacheFile(memMgr, outs3.getCloudFile());
					finalOutput = new CacheOutputStream(cFile, outs3, async, this);
                }
                case DISK_ONLY -> {      //Disk  + s3
					if (existingDiskFile != null) {
						policyNotifier.notifyItemUpdate(existingDiskFile, StoreOptionType.DISK_ONLY);
					}
					//if (existingMemoryFile != null) {
						CacheFileHelper.handleDelete(bucket, key, memMgr);
					//}
					if (logger.isDebugEnabled())
						logger.info("CREATE: DISK ONLY");
                    CacheFile cFile = CacheFileHelper.createTierCacheFile(dmgr, outs3.getCloudFile());
					finalOutput = new CacheOutputStream(cFile, outs3, async, this);
				}
                case MEMORY_DISK -> {    //Memory + Disk  + s3
                    if (existingMemoryFile != null) {
                        policyNotifier.notifyItemUpdate(existingMemoryFile, StoreOptionType.MEMORY_ONLY);
                    }
                    if (existingDiskFile != null) {
                        policyNotifier.notifyItemUpdate(existingDiskFile, StoreOptionType.DISK_ONLY);
                    }
                    if (logger.isDebugEnabled()) logger.info("CREATE: MEM + DISK");
                    CacheFile memCacheFile = CacheFileHelper.createTierCacheFile(memMgr, outs3.getCloudFile());
                    CacheFile diskCacheFile = CacheFileHelper.createTierCacheFile(dmgr, outs3.getCloudFile());
                    finalOutput = new CacheOutputStream(memCacheFile, diskCacheFile, outs3, async, this);
                }
                case S3_ONLY -> {          // ONLY s3
                    if (logger.isDebugEnabled()) logger.info("CREATE: S3 ONLY [SYNC MODE]");
                    finalOutput = new CacheOutputStream(outs3, this);    //OUT TO S3
                    if (existingMemoryFile == null && existingDiskFile == null ) {
                        policyNotifier.notifyItemNotAdded(s3CacheFile, StoreOptionType.S3_ONLY);
                    } //else {
                        // Delete from cache if it exists
                        DeleteResult deleteResult = deleteFileFromCache(bucket, key);
                        if (deleteResult.wasDeletedSuccessfully()) {
                            if (logger.isDebugEnabled()) logger.info("DELETED FROM CACHE DUE TO S3_ONLY");
                      //  }
                    }
                }
                default -> {
                    //LOG CRITICAL
                    logger.fatal("Switch Statement Error: Unknown case argument");
                    throw new IOException("Incorrect argument is given <<<<< ERROR <<<<<<");
                }
            }
        } catch (FileNotFoundException e) {
            throw new IOException("File Not Found On Creation");
        }    //if file is not actually created by write
        catch (IOException ex) {
            throw new IOException(ex);
        }
        result.setCacheOutputStream(finalOutput);
        return result;
    }

    // Read a whole file from the cache (if exists), or from S3 (if not)
    public GetResult read(String bucket, String key, CloudInfo cloudInfo) throws IOException {

        GetResult result = new GetResult();
        InputStream returnIS = null;    //returnInputStream

        // Try to read from memory
        if (memMgr.isActive()) {
            if (logger.isDebugEnabled()) logger.info("Checking Memory");
            returnIS = memMgr.read(bucket, key);
        }

        // If not found in memory, try to read from disk
        if (returnIS == null && dmgr.isActive()) {
            if (logger.isDebugEnabled()) logger.info("Checking Disk");
            returnIS = dmgr.read(bucket, key);
        }

        // If not found in memory or disk, try to read from S3
        if (returnIS == null) {
            if (logger.isDebugEnabled()) logger.info("Checking S3");
            CloudFileReader s3IS = cloudMgr.read(bucket, key, cloudInfo);

            if (s3IS != null) {
                /* Note:  Despite the fact that we may already have a partial file on disk, we agreed to not use the existing cache file, and create a new one. This a  limitation of the architecture of the cache system */
                returnIS = cacheS3Read(s3IS);
            }
        }
        if (returnIS == null)
            throw new IOException("File does not exist");

        result.setInputStream(returnIS);
        return result;
    }

    private InputStream cacheS3Read(CloudFileReader s3IS) throws IOException {
        CacheOutputStream wout;
        CacheFile s3CacheFile = CacheFileHelper.createS3CacheFile(s3IS.getCloudFile());
        CacheFile dfile, mfile;
        InputStream returnIS;

        StoreOptionType selectedStoreOption = admissionPolicy.getReadAdmissionLocation(s3CacheFile);
        selectedStoreOption = ensureValidStoreOptionType(selectedStoreOption);
        switch (selectedStoreOption) {
            case MEMORY_ONLY -> {    //Read + save to memory
                if (logger.isDebugEnabled()) logger.info("READ S3 -> WRITE MEMORY");
                mfile = CacheFileHelper.createTierCacheFile(memMgr, s3IS.getCloudFile());
                wout = new CacheOutputStream(mfile, this);
                wout.setCloudFileReader(s3IS);
                returnIS = new InputStreamCacheSplitter(s3IS, wout);
            }
            case DISK_ONLY -> {    //Read + save to Disk
                if (logger.isDebugEnabled()) logger.info("READ S3 -> WRITE DISK");
                dfile = CacheFileHelper.createTierCacheFile(dmgr, s3IS.getCloudFile());
                wout = new CacheOutputStream(dfile, this);
                wout.setCloudFileReader(s3IS);
                returnIS = new InputStreamCacheSplitter(s3IS, wout);
            }
            case MEMORY_DISK -> {    //save to both
                if (logger.isDebugEnabled()) logger.info("READ S3 -> WRITE DISK + MEMORY");
                mfile = CacheFileHelper.createTierCacheFile(memMgr, s3IS.getCloudFile());
                dfile = CacheFileHelper.createTierCacheFile(dmgr, s3IS.getCloudFile());
                wout = new CacheOutputStream(mfile, dfile, this);
                wout.setCloudFileReader(s3IS);
                returnIS = new InputStreamCacheSplitter(s3IS, wout);
            }
            case S3_ONLY -> {
                //do not cache the reading stream
                returnIS = s3IS;    //just read. do not store anywhere in cache
                policyNotifier.notifyItemNotAdded(s3CacheFile, StoreOptionType.S3_ONLY);
            }
            default -> {
                //LOG CRITICAL
                logger.fatal("Switch Statement Error: Unknown case argument");
                throw new IOException("Incorrect arugment is given <<<<< ERROR <<<<<<");
            }
        }
        return returnIS;
    }

    /**
     * Check if the provided store option type is valid based on the cache managers that are active.
     * If not, correct it.
     * @param type
     * @return
     */
    private StoreOptionType ensureValidStoreOptionType(StoreOptionType type) {
        StoreOptionType result = type;
        switch (type) {
            case MEMORY_ONLY:
                if (!memMgr.isActive())
                    result = StoreOptionType.S3_ONLY;
                break;
            case DISK_ONLY:
                if (!dmgr.isActive())
                    result = StoreOptionType.S3_ONLY;
                break;
            case MEMORY_DISK:
                if (!memMgr.isActive() && dmgr.isActive())
                    result = StoreOptionType.DISK_ONLY;
                else if (memMgr.isActive() && !dmgr.isActive())
                    result = StoreOptionType.MEMORY_ONLY;
                else if (!memMgr.isActive() && !dmgr.isActive())
                    result = StoreOptionType.S3_ONLY;
                break;
            case S3_ONLY:
                break;
        }

        return result;
    }

    // Read a range of data from the cache (if exists), or from S3 (if not)
    public GetResult read(String bucket, String key, long start, long stop, CloudInfo cloudInfo) throws IOException {
        if (stop < start)
            throw new IOException("Illegal Range Specification: Stop < Start");

        GetResult result = new GetResult();
        InputStream returnIS = null;    //returnInputStream

        if (memMgr.isActive()) {
            if (logger.isDebugEnabled()) logger.info("Checking Memory");
            returnIS = memMgr.read(bucket, key, start, stop);
        }

        if (returnIS == null && dmgr.isActive()) {
            if (logger.isDebugEnabled()) logger.info("Checking Disk");
            returnIS = dmgr.read(bucket, key, start, stop);
        }

        if (returnIS == null) {
            if (logger.isDebugEnabled()) logger.info("Checking S3 & Saving to cache");

            CacheOutputStream wout;
            CacheManager cacheDiskManager = null, cacheMemoryManager = null;

            CloudFileReader s3IS = cloudMgr.read(bucket, key, start, stop, cloudInfo);

            if (s3IS != null) {
                CacheFile s3CacheFile = CacheFileHelper.createS3CacheFile(s3IS.getCloudFile());
                StoreOptionType selectedStoreOption = admissionPolicy.getReadAdmissionLocation(s3CacheFile);
                selectedStoreOption = ensureValidStoreOptionType(selectedStoreOption);
                switch (selectedStoreOption) {
                    case MEMORY_DISK:
                        cacheMemoryManager = memMgr;
                        cacheDiskManager = dmgr;
                        if (logger.isDebugEnabled()) {
                            logger.info("READ S3 -> WRITE DISK + MEMORY");
                        }
                        break;
                    case DISK_ONLY:    //Read + save to Disk Only
                        if (logger.isDebugEnabled()) {
                            logger.info("READ S3 -> WRITE DISK");
                        }
                        cacheDiskManager = dmgr;
                        break;
                    case MEMORY_ONLY:
                        if (logger.isDebugEnabled()) {
                            logger.info("READ S3 -> WRITE MEMORY");
                        }
                        cacheMemoryManager = memMgr;
                        break;
                    case S3_ONLY:
                        if (logger.isDebugEnabled()) {
                            logger.info("READ S3 -> No Caching");
                        }
                        break;
                    default:
                        //LOG CRITICAL
                        logger.fatal("Switch Statement Error: Unknown case argument");
                        throw new IOException("Incorrect arugment is given <<<<< ERROR <<<<<<");
                }

                // The policies are notified inside this method
                CacheFile diskFile = getReadCacheFile(cacheDiskManager, s3IS);
                CacheFile memFile = getReadCacheFile(cacheMemoryManager, s3IS);

                if (diskFile != null && memFile != null) {
                    // Cache to both disk and memory
                    wout = new CacheOutputStream(diskFile, memFile, start, stop, this);
                }
                else if (diskFile != null) {
                    // Cache only to disk
                    wout = new CacheOutputStream(diskFile, start, stop, this);
                }
                else if (memFile != null) {
                    // Cache only to memory
                    wout = new CacheOutputStream(memFile, start, stop, this);
                }
                else {
                    // Do not cache, but the policies must be updated
                    policyNotifier.notifyItemNotAdded(s3CacheFile, StoreOptionType.S3_ONLY);
                    wout = null;
                }

                if (wout != null) {
                    wout.setCloudFileReader(s3IS);
                    returnIS = new InputStreamCacheSplitter(s3IS, wout);
                } else
                    returnIS = s3IS;
            }
        }

        if (returnIS == null) {
            throw new IOException("File does not exist");
        }

        result.setInputStream(returnIS);
        return result;
    }

    private CacheFile getReadCacheFile(CacheManager cmgr, CloudFileReader s3reader) throws IOException {
        if (cmgr == null) {
            return null;
        }
        String bucket = s3reader.getCloudFile().getBucket();
        String key = s3reader.getCloudFile().getKey();
        CacheFile cacheFile = cmgr.getFile(bucket, key);
        if (cacheFile == null) {
            logger.info("Local file not in range - Creating new one & fetching from s3");
            cacheFile = CacheFileHelper.createTierCacheFile(cmgr, s3reader.getCloudFile());
            return cacheFile;
        }

        if (cacheFile.isPartialFile()) {
            logger.info("PARTIAL + owned: " + s3reader.getCloudFile().isOwnedFile());
        }
        else if (isPending(bucket, key)) {
            throw new IOException("File Pending Upload <-> File not found (or incorrect range)");
        }

        if (!s3reader.getCloudFile().isOwnedFile()) {
            // Create a new file
            logger.info("Local file not owned - Creating new one & fetching from s3");
            cacheFile = CacheFileHelper.createTierCacheFile(cmgr, s3reader.getCloudFile());
        }
        return cacheFile;
    }

    InputStream dumpRead(String bucket, String key, CloudInfo cloudInfo) throws Exception {
        if (logger.isDebugEnabled()) logger.info("Checking S3");
        CloudFileReader s3IS = cloudMgr.read(bucket, key, cloudInfo);
        InputStream returnIS = null;
        if (s3IS != null) {
            returnIS = cacheS3Read(s3IS);
        }
        return returnIS;
    }

    public TierGeneralStatistics getMemoryStatistics() {
        return memMgr.getTierStatistics();
    }

    public TierGeneralStatistics getDiskStatistics() {
        return dmgr.getTierStatistics();
    }

    public PerformanceStatistics getStoragePerformanceStatistics() {
        return (PerformanceStatistics) storageStatistics;
    }

    DiskManager getDiskManager() {
        return dmgr;
    }

    public CachePolicyNotifier getCachePolicyNotifier() {
        return policyNotifier;
    }

    public EvictionTriggerPolicy getEvictionTriggerPolicy() {
        return evictionTriggerPolicy;
    }

    private void addPending(String bucket, String key) {
        synchronized (pendingUploads) {
            if (!pendingUploads.containsKey(bucket))
                pendingUploads.put(bucket, new HashMap<>());

            HashMap<String, AtomicInteger> innerPendingUploads = pendingUploads.get(bucket);
            if (innerPendingUploads.containsKey(key))
                innerPendingUploads.get(key).incrementAndGet();
            else
                innerPendingUploads.put(key, new AtomicInteger(1));
        }
    }

    void removePending(String bucket, String key) {
        synchronized (pendingUploads) {
            if (pendingUploads.containsKey(bucket) && pendingUploads.get(bucket).containsKey(key))
                if (pendingUploads.get(bucket).get(key).decrementAndGet() <= 0)
                    pendingUploads.get(bucket).remove(key);
        }
    }

    boolean isPending(String bucket, String key) {
        synchronized (pendingUploads) {
            if (pendingUploads.containsKey(bucket) && pendingUploads.get(bucket).containsKey(key))
                return pendingUploads.get(bucket).get(key).intValue() > 0;
            else
                return false;
        }
    }

    void checkVersion(String bucket, String key, long newVersion) {
        // check version if and only to managers that are configured
        if (dmgr.isActive()) dmgr.checkVersion(bucket, key, newVersion);
        if (memMgr.isActive()) memMgr.checkVersion(bucket, key, newVersion);
        //WE SHOULD NOT call checkVersion for s3, since new file, will overwrite the s3 file eventually.
    }

    public DeleteResult delete(String bucket, String key, CloudInfo cloudInfo) throws IOException {
        DeleteResult result = new DeleteResult();
        boolean d;
        CloudFile cloudFile = cloudMgr.statFile(bucket, key, cloudInfo);
        // CASE: Doesn't exist in S3
        if (cloudFile == null) {
            // Try to delete from cache
            result = deleteFileFromCache(bucket, key);
            return result;
        }

        // CASE: Exists in S3
        d = cloudMgr.delete(bucket, key, cloudInfo);
        if (d) {
            result = deleteFileFromCache(bucket, key);
            CacheFile s3File = CacheFileHelper.createS3CacheFile(cloudFile);
            result.setS3File(s3File);
            result.setSuccessfulDeletion();
        }
        return result;
    }

    // Delete file from cache
    public DeleteResult deleteFileFromCache(String bucket, String key) {
        DeleteResult result = new DeleteResult();
        boolean delMem = false, delDisk = false;
        if (memMgr.isActive()) {
            if (memMgr.containsObject(bucket, key)) {
                result.addCacheFile(memMgr.getFile(bucket, key));
            }
            delMem = CacheFileHelper.handleDelete(bucket, key, memMgr);
        }
        if (dmgr.isActive()) {
            if (dmgr.containsObject(bucket, key)) {
                result.addCacheFile(dmgr.getFile(bucket, key));
            }
            delDisk = CacheFileHelper.handleDelete(bucket, key, dmgr);
        }
        try {
            removeVersion(bucket, key);
        } catch (Exception ignored) {
        }    //in case someone removed already the versioncounter key

        if (delMem || delDisk) {
            result.setSuccessfulDeletion();
        }
        return result;
    }

    public List<SMACCObject> list(String bucket, String prefix) {
        Map<String, SMACCObject> objectMap = new HashMap<>();

        if (memMgr.isActive())
            list(bucket, prefix, objectMap, memMgr.list(bucket));
        if (dmgr.isActive())
            list(bucket, prefix, objectMap, dmgr.list(bucket));

        return new ArrayList<>(objectMap.values());
    }

    private void list(String bucket, String prefix, Map<String, SMACCObject> objectMap, Set<CacheFile> cacheFiles) {
        /* Possible double cacheFiles can be listed (if they exist in mem and disk) */
        for (CacheFile file : cacheFiles) {
            if (objectMap.containsKey(file.getKey())) {
                // In case the file is already in the map, it means it is in memory and disk
                objectMap.get(file.getKey()).setStoreOptionType(StoreOptionType.MEMORY_DISK);
            } else {
                if (prefix == null) {
                    objectMap.put(file.getKey(), new SMACCObject(file));
                } else {
                    if (file.getKey().startsWith(prefix))
                        objectMap.put(file.getKey(), new SMACCObject(file));
                }
            }
        }
    }

    public SMACCObject getSMACCObject(String bucket, String key) {
        CacheFile file = null;
        // Try to get from memory first
        if (memMgr.isActive()) file = memMgr.getFile(bucket, key);
        // If it exists in memory and disk
        if (file != null && dmgr.isActive()) {
            CacheFile diskFile = dmgr.getFile(bucket, key);
            if (diskFile != null) {
                SMACCObject smaccObject = new SMACCObject(file);
                smaccObject.setStoreOptionType(StoreOptionType.MEMORY_DISK);
                return smaccObject;
            }
        }

        // Try to get from disk
        if (file == null && dmgr.isActive()) file = dmgr.getFile(bucket, key);

        // Object doesn't exist in SMACC cache
        if (file == null) return null;

        return new SMACCObject(file);
    }

    private boolean initiateRecovery() {
        //	Start pool for asynchronous transfer of files to cache using s3 or disk
        ExecutorService recoveryService = Executors.newFixedThreadPool(ServerConfigurations.getMemoryRecoverServicePoolSize());

        try {
            // Initiate Disk (initiate before memory - because we may need to retrieve data from disk and load them into memory)
            HashMap<String, DiskFile> diskRecoveryList = new HashMap<>();
            if (dmgr.isActive()) diskRecoveryList = dmgr.initiateRecovery();

            if (diskRecoveryList == null) return false;

            // Send pushed disk files for s3 async uploading
            Set<String> dset = new HashSet<>(diskRecoveryList.keySet());
            Iterator<String> diter = dset.iterator();
            while (diter.hasNext()) {
                String bucketKey = diter.next();
                //	Get Disk File
                DiskFile file = diskRecoveryList.get(bucketKey);

                //	Set global version using restored disk files
                setCurrentVersion(file.getBucket(), file.getKey(), file.getVersion());
                CloudInfo cloudInfo = cloudMgr.getCloudInfoForBucket(file.getBucket());
                if (file.isPushed() && cloudInfo != null) {
                    // OUT TO S3
                    CloudFileWriter outs3 = cloudMgr.create(false, file.getBucket(), file.getKey(), null, cloudInfo);
                    CacheOutputStream finalOutput = new CacheOutputStream();
                    finalOutput.initiateRecovery(outs3, file, this);
                    finalOutput.close();
                } else if (file.isComplete() && cloudInfo != null) {
                    // Retrieve s3 meta data of file
                    CloudFile cloudFile = cloudMgr.statFile(file.getBucket(), file.getKey(), cloudInfo);
                    if (cloudFile == null) {
                        /* Metadata retrieval of file failed. Delete cache file */
                        logger.info("Recovery: Metadata fetching failed");
                        diskRecoveryList.remove(bucketKey);
                        diter.remove();

                        CacheFileHelper.handleDelete(file.getBucket(), file.getKey(), dmgr);
                        continue;
                    }
                    if (cloudFile.isOwnedFile()) {
                        file.setActualSize(cloudFile.getLength());
                        file.setLastModified(cloudFile.getLastModified());
                    } else {

                        /* Delete File if cache file is no longer on s3 (or s3 has different, newer file) */
                        diskRecoveryList.remove(bucketKey);
                        diter.remove();
                        String bucket = file.getBucket();
                        String key = file.getKey();

                        CacheFileHelper.handleDelete(bucket, key, dmgr);

                        //	Create a new Disk File and download new file from s3
                        try {
                            CloudFileReader reader = cloudMgr.read(bucket, key, cloudInfo);
                            if (reader != null) {
                                DiskFile newFile = dmgr.create(bucket, key);
                                newFile.setActualSize(reader.getCloudFile().getLength());
                                CacheOutputStream blockOutput = new CacheOutputStream(newFile, this);
                                recoveryService.execute(new BlockTransferProcessor(this, reader, blockOutput));
                            } else if (logger.isDebugEnabled())
                                logger.info("Required file does not exist in s3 - Skipping file");
                        } catch (IOException e) // we have to catch exception here because in the same loop we load
                                                // files from disk too
                        {
                            if (logger.isDebugEnabled())
                                logger.info("Failed to download file from s3: " + e.getMessage());
                        }

                    }
                }
            }

            // Initiate Memory
            HashMap<String, ArrayList<FilenameFeatureExtractor>> memObjList = new HashMap<>();
            if (memMgr.isActive()) memObjList = memMgr.initiateRecovery();
            if (memObjList == null) return false;

            //	Set global version using restored memory state files
            for (ArrayList<FilenameFeatureExtractor> featList : memObjList.values()) {
                FilenameFeatureExtractor feature = featList.get(0); // All features has the same version
                setCurrentVersion(feature.getBucket(), feature.getKey(), feature.getVersion());
            }

            // Recover Memory Using Disk - Asynchronously
            ArrayList<ArrayList<FilenameFeatureExtractor>> allMemoryStates = new ArrayList<>(memObjList.values());
            Iterator<ArrayList<FilenameFeatureExtractor>> iter = allMemoryStates.iterator();
            DiskFile dfile;
            MemoryFile memFile;
            while (iter.hasNext()) {
                ArrayList<FilenameFeatureExtractor> memoryStateBlocks = iter.next();
                FilenameFeatureExtractor memBlockFeat = memoryStateBlocks.get(0);
                long memBlockVersion = memBlockFeat.getVersion();
                String bucket = memBlockFeat.getBucket();
                String key = memBlockFeat.getKey();
                memFile = null;

                for (FilenameFeatureExtractor memBlockFeature : memoryStateBlocks) {
                    long start = memBlockFeature.getRange().getStart();
                    long stop = memBlockFeature.getRange().getStop();

                    dfile = null;
                    if (dmgr.isActive()) dfile = diskRecoveryList.get(bucket + key);
                    if (dfile != null && dfile.getVersion() == memBlockVersion) {
                        //	Load all memory blocks using a disk file asynchronously (if available in same version)
                        if (dfile.inRange(start, stop)) {
                            if (memFile == null) {
                                memFile = memMgr.create(bucket, key);
                                memFile.setPartialFile();
                                memFile.setVersion(memBlockVersion);
                            }
                            CacheOutputStream blockOutput = new CacheOutputStream(memFile, start, stop, this);
                            InputStream blockInput = dmgr.read(bucket, key, start, stop);

                            /* Transfer Block asynchronously */
                            recoveryService.execute(new BlockTransferProcessor(this, blockInput, blockOutput));

                        }
                    } else    //	Load all memory blocks using S3 asynchronously (if available)
                    {
                        //	Create a new Memory File (using a new version)
                        try {
                            CloudInfo cloudInfo = cloudMgr.getCloudInfoForBucket(bucket);
                            if (cloudInfo != null) {
                                CloudFileReader reader = cloudMgr.read(bucket, key, start, stop, cloudInfo);
                                if (reader != null) {
                                    memFile = (MemoryFile) memMgr.create(bucket, key,
                                            reader.getCloudFile().getLength());
                                    CacheOutputStream blockOutput = new CacheOutputStream(memFile, start, stop, this);
                                    recoveryService.execute(new BlockTransferProcessor(this, reader, blockOutput));
                                } else if (logger.isDebugEnabled())
                                    logger.info("Required file does not exist in s3 - Skipping file");
                            } else if (logger.isDebugEnabled())
                                logger.info("Required credentials not found - Skipping file");
                        } catch (IOException e)    //we have to catch exception here  because in the same loop we load files from disk too
                        {
                            if (logger.isDebugEnabled()) logger.info("Failed to download file from s3");
                        }

                        //	File failed to load. Delete state file
                        if (ServerConfigurations.deleteStateFileOnFailure())
                            try {
                                if (new File(memBlockFeature.getFilename()).delete()) {
                                    if (logger.isDebugEnabled())
                                        logger.info("Deleted state file that failed to load: " + memBlockFeature.getFilename());
                                }
                            } catch (Exception e) { /* Do nothing */ }
                    }
                }
            }


            recoveryService.shutdown();
            return true;
        } catch (IOException exc) {
            logger.fatal(exc);
            return false;
        }
    }

    private void initiate(Configuration configuration) {
        if (logger.isDebugEnabled()) logger.info("================= INITIALIZATION ==================");

        //	Load Server Configurations
        try {
            ServerConfigurations.initialize(configuration);
            ServerConfigurations.configsSet();    /*  Busy Waiting until configuration values are set using config file */
            if (ServerConfigurations.getMasterSecretKey() == null || ServerConfigurations.getMasterAccessKey() == null) {
                logger.error("Master Credentials not found");
                System.exit(1);
            }
        } catch (ConfigurationException e) {
            logger.fatal("Server Configurations Initialization Failed ", e);
            System.exit(1);
        }

        pendingUploads = new HashMap<>();

        HashMap<Integer, StoreSettings> diskSettings = ServerConfigurations.getServerDiskVolumes();
        StoreSettings memorySettings = ServerConfigurations.getServerMemorySettigs();

        // Create a first s3client based on default settings
        String defaultBucket = ServerConfigurations.getDefaultBucket();
        CloudInfo defaultCloudInfo = ServerConfigurations.getDefaultCloudInfo();

        // Initiate Cloud Manager
        if (ServerConfigurations.getBackendCloudStorage().equalsIgnoreCase("S3")) {
            this.cloudMgr = new S3Manager();
        } else if (ServerConfigurations.getBackendCloudStorage().equalsIgnoreCase("MinIO")) {
            this.cloudMgr = new MinioManager();
        } else {
            logger.fatal("Unknown backend cloud storage system. Supported: S3 and MinIO");
            System.exit(-1);
        }

        if (!cloudMgr.initiate(this, defaultBucket, defaultCloudInfo))
            return;

        // Initialize the admission and eviction policies
        admissionPolicy = AdmissionPolicy.getInstance(configuration);
        EvictionItemPolicy evictionItemPolicy = EvictionItemPolicy.getInstance(configuration);
        EvictionPlacementPolicy evictionPlacementPolicy = EvictionPlacementPolicy.getInstance(configuration);
        policyNotifier = CachePolicyNotifier.createNotifierFromPoliciesList(
                List.of(admissionPolicy, evictionItemPolicy, evictionPlacementPolicy));

        evictionTriggerPolicy = EvictionTriggerPolicy.getInstance(configuration);

        //	Start downgration handler
        downgrationHandler = Executors.newFixedThreadPool(ServerConfigurations.getDowngrationHanlderTheadPoolSize());

        this.dmgr = new DiskManager(diskSettings, configuration, policyNotifier);
        this.memMgr = new MemoryManager(memorySettings, policyNotifier);
        storageStatistics = new PerformanceStatistics();
        StatisticsTimeUpdaterOnOperation timeUpdater = new StatisticsTimeUpdaterOnOperation(storageStatistics);
        storageStatistics.setParentUpdater(timeUpdater);

        //	Start cache Eviction Handler
        evictionManager = new EvictionManager(downgrationHandler, this, dmgr, memMgr, diskSettings, memorySettings);
        Thread ehThread = new Thread(evictionManager);
        ehThread.start();

        if (ServerConfigurations.getCacheRecoveryActivate()) {
            if (!initiateRecovery()) // Must initiate s3 manager before recovery
            {
                logger.fatal("Recovery Failed!");
                return;
            }

            recoveryDone = true;
        }

        if (logger.isDebugEnabled()) logger.info("===================================================");
    }

    boolean isRecoveryDone() {
        return recoveryDone;
    }

    public void shutdown() {
        if (memMgr.isActive())
            this.memMgr.shutdown();
        cloudMgr.shutdown(); // S3 must shutdown before disk mgr - Blocks
        if (dmgr.isActive())
            this.dmgr.shutdown();
        downgrationHandler.shutdownNow();
        evictionManager.shutdown();
    }

    public CloudInfo getCloudInfoForBucket(String bucket) {
        return cloudMgr.getCloudInfoForBucket(bucket);
    }
}
