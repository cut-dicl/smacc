package edu.cut.smacc.server.cache.policy.admission;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.common.io.ByteBufferPool;
import edu.cut.smacc.server.cache.disk.DiskManager;
import edu.cut.smacc.server.cache.memory.MemoryBlock;
import edu.cut.smacc.server.cache.memory.MemoryManager;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import edu.cut.smacc.utils.collections.WeightedEXDNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;

public class AdmissionEXDTest {

    @Test
    void testAdmit() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.addProperty(ServerConfigurations.CACHING_POLICY_WEIGHTED_BIAS_HOURS_KEY,
                ServerConfigurations.CACHING_POLICY_WEIGHTED_BIAS_HOURS_DEFAULT);
        conf.addProperty(ServerConfigurations.UPLOAD_LOCATION_KEY,
                "MEMORYDISK");
        conf.addProperty(ServerConfigurations.REQUEST_LOCATION_KEY,
                "MEMORYDISK");
        // memory capacity is 100
        conf.addProperty(ServerConfigurations.CACHE_MEMORY_CAPACITY_KEY, 100);
        // disk 1 capacity is 1000
        conf.addProperty("cache.disk.volume.0", "cache/DiskData1/, cache/DiskState1, 1000");
        // disk 2 capacity is 1200
        conf.addProperty("cache.disk.volume.1", "cache/DiskData2/, cache/DiskState2/, 1200");
        AdmissionPolicy policy = new AdmissionEXD();

        // Memory is there by default, we only have to add disk explicitly
        conf.addProperty(ServerConfigurations.CACHE_DISK_VOLUMES_SIZE_KEY, 2);

        // Initialize cache setting using reflection
        Class<?> clazz = ServerConfigurations.class;
        Method method = clazz.getDeclaredMethod("initializeCacheSettings", Configuration.class);
        method.setAccessible(true);
        method.invoke(null, conf);

        policy.initialize(conf);

        WeightedEXDNode.ALPHA = 0.01;

        StoreSettings memSettings = ServerConfigurations.getServerMemorySettigs();
        HashMap<Integer, StoreSettings> diskSettings = ServerConfigurations.getServerDiskVolumes();
        CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy));
        MemoryManager memManager = new MemoryManager(memSettings, notifier);
        DiskManager diskManager = new DiskManager(diskSettings, conf, notifier);
        ByteBufferPool pool = new ByteBufferPool(2048);

        // Case 1: 99 bytes file admission
        CacheFile cacheFile = createCacheFiles("test-key", 99, memManager, diskManager);
        // Adding a block to the cache file
        CacheBlock block = new MemoryBlock(0, 98, memSettings.getStateFolder(), pool, cacheFile, memSettings.getStats());
        addCacheBlock(block, cacheFile);
        assert getWriteAdmissionLocation(policy, memSettings, diskSettings, cacheFile) == StoreOptionType.MEMORY_DISK;

        // Case 2: Trying to add one more file with 200 bytes must only be added to the disk
        CacheFile cacheFile2 = createCacheFiles("test-key2", 200, memManager, diskManager);
        // Adding a block to the cache file
        block = new MemoryBlock(0, 199, memSettings.getStateFolder(), pool, cacheFile2, memSettings.getStats());
        addCacheBlock(block, cacheFile);
        assert getWriteAdmissionLocation(policy, memSettings, diskSettings, cacheFile2) == StoreOptionType.DISK_ONLY;

        // Case 3: Adding a 50 byte file
        CacheFile cacheFile3 = createCacheFiles("test-key3", 50, memManager, diskManager);
        // Adding a block to the cache file
        block = new MemoryBlock(0, 49, memSettings.getStateFolder(), pool, cacheFile3, memSettings.getStats());
        addCacheBlock(block, cacheFile);
        Thread.sleep(50); // Add some busy waiting time for the weights
        // This file must be added to both memory and disk, because it has more weight than the file in case 1
        assert getWriteAdmissionLocation(policy, memSettings, diskSettings, cacheFile3) == StoreOptionType.MEMORY_DISK;
        // Admitting this means the other gets evicted, so it would decrease the memory capacity by 99
        memSettings.getStats().decrement(99, 99);

        // Case 4: Adding a 200 bytes file with lower weight than file 3
        CacheFile cacheFile4 = createCacheFiles("test-key4", 200, memManager, diskManager);
        // Adding a block to the cache file
        block = new MemoryBlock(0, 199, memSettings.getStateFolder(), pool, cacheFile4, memSettings.getStats());
        addCacheBlock(block, cacheFile);
        // This file must be added only to disk, because it has lower weight than the file in case 3
        assert getWriteAdmissionLocation(policy, memSettings, diskSettings, cacheFile4) == StoreOptionType.DISK_ONLY;

        // Case 5: Every cache tier is full, so the next file must not be cached
        // Fill up the disks with a 451 bytes file for D1 and 2 files for D2:800 and 200 bytes
        CacheFile cacheFile5 = createCacheFiles("test-key5", 451, memManager, diskManager);
        // Adding a block to the cache file
        block = new MemoryBlock(0, 450, memSettings.getStateFolder(), pool, cacheFile5, memSettings.getStats());
        addCacheBlock(block, cacheFile);
        assert getWriteAdmissionLocation(policy, memSettings, diskSettings, cacheFile5) == StoreOptionType.DISK_ONLY;

        CacheFile cacheFile6 = createCacheFiles("test-key6", 800, memManager, diskManager);
        // Adding a block to the cache file
        block = new MemoryBlock(0, 799, memSettings.getStateFolder(), pool, cacheFile6, memSettings.getStats());
        addCacheBlock(block, cacheFile);
        assert getWriteAdmissionLocation(policy, memSettings, diskSettings, cacheFile6) == StoreOptionType.DISK_ONLY;

        CacheFile cacheFile7 = createCacheFiles("test-key7", 200, memManager, diskManager);
        // Adding a block to the cache file
        block = new MemoryBlock(0, 199, memSettings.getStateFolder(), pool, cacheFile7, memSettings.getStats());
        addCacheBlock(block, cacheFile);
        assert getWriteAdmissionLocation(policy, memSettings, diskSettings, cacheFile7) == StoreOptionType.DISK_ONLY;

        // Create a final 300 bytes file
        CacheFile cacheFile8 = createCacheFiles("test-key8", 1500, memManager, diskManager);
        // Adding a block to the cache file
        block = new MemoryBlock(0, 1499, memSettings.getStateFolder(), pool, cacheFile8, memSettings.getStats());
        addCacheBlock(block, cacheFile);
        // This file must not be cached anywhere
        assert getWriteAdmissionLocation(policy, memSettings, diskSettings, cacheFile8) == StoreOptionType.S3_ONLY;

        // Get read admission location
        assert getReadAdmissionLocation(policy, memSettings, diskSettings, cacheFile) == StoreOptionType.MEMORY_DISK;

        System.out.println("AdmissionEXDTest.testAdmit() passed");
    }

    private CacheFile createCacheFiles(String key, int size,
                                       MemoryManager memManager, DiskManager diskManager) throws IOException {
        CacheFile memCacheFile = memManager.create("test-bucket", key, size);
        CacheFile diskCacheFile = diskManager.create("test-bucket", key, size);
        memManager.put("test-bucket", key, memCacheFile);
        diskManager.put("test-bucket", key, diskCacheFile);
        return memCacheFile;
    }

    private void addCacheBlock(CacheBlock block, CacheFile cacheFile) {
        cacheFile.addIncompleteBlock(block);
        cacheFile.makeBlockVisible(block);
        cacheFile.stateComplete();
    }

    private StoreOptionType getWriteAdmissionLocation(AdmissionPolicy policy, StoreSettings memSettings,
                                        HashMap<Integer, StoreSettings> diskSettings, CacheFile file) {
        StoreOptionType location = policy.getWriteAdmissionLocation(file);
        if (location == StoreOptionType.MEMORY_DISK) {
            memSettings.getStats().increment(file.getActualSize());
            incrementDiskStats(diskSettings, file.getActualSize());
            diskSettings.get(0).getStats().increment(file.getActualSize());
        } else {
            if (location == StoreOptionType.MEMORY_ONLY) {
                memSettings.getStats().increment(file.getActualSize());
            } else if (location == StoreOptionType.DISK_ONLY) {
                incrementDiskStats(diskSettings, file.getActualSize());
            }
        }

        return location;
    }

    private StoreOptionType getReadAdmissionLocation(AdmissionPolicy policy, StoreSettings memSettings,
                                                      HashMap<Integer, StoreSettings> diskSettings, CacheFile file) {
        StoreOptionType location = policy.getReadAdmissionLocation(file);
        if (location == StoreOptionType.MEMORY_DISK) {
            policy.onItemAccess(file, StoreOptionType.MEMORY_ONLY);
            policy.onItemAccess(file, StoreOptionType.DISK_ONLY);
        } else {
            policy.onItemAccess(file, location);
        }
        memSettings.getStats().increment(file.getSize());
        diskSettings.get(0).getStats().increment(file.getSize());
        return location;
    }

    private void incrementDiskStats(HashMap<Integer, StoreSettings> diskSettings, long size) {
        // Find max capacity disk
        int maxDisk = 0;
        for (int i = 1; i < diskSettings.size(); i++) {
            if (diskSettings.get(i).getStats().getMaxCapacity() >
                    diskSettings.get(maxDisk).getStats().getMaxCapacity()) {
                maxDisk = i;
            }
        }
        diskSettings.get(maxDisk).getStats().increment(size);

    }

}
