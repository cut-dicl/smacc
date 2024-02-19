package edu.cut.smacc.server.cache.policy.eviction;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.common.io.ByteBufferPool;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.memory.MemoryBlock;
import edu.cut.smacc.server.cache.memory.MemoryManager;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import edu.cut.smacc.server.cache.policy.eviction.trigger.ThresholdPercentageTrigger;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemLFU;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import edu.cut.smacc.server.cache.policy.eviction.trigger.EvictionTriggerPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EvictionItemLFUTest {

    @Test
    void testEvict() throws IOException {
        // Creating a memory file with no parent
        String bucket = "test-bucket";
        String mainFolder = "";
        String stateFolder = "";
        UsageStats usageStats = new UsageStats(1124);
        StoreSettings settings = new StoreSettings(mainFolder, stateFolder, usageStats);

        // Trigger evictions at 90%
        Configuration configuration = new Configuration();
        configuration.addProperty(ServerConfigurations.EVICTION_TRIGGER_POLICY_KEY,
                ThresholdPercentageTrigger.class.getName());
        configuration.addProperty(ServerConfigurations.EVICTION_TRIGGER_POLICY_THRESHOLD_KEY,
                90);
        configuration.addProperty(ServerConfigurations.EVICTION_ITEM_POLICY_KEY,
                EvictionItemLFU.class.getName());

        ByteBufferPool pool = new ByteBufferPool(2048);

        // Initialize the eviction policy (LFU)
        EvictionItemPolicy policy = new EvictionItemLFU();
        policy.initialize(configuration);
        CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy));
        MemoryManager manager = new MemoryManager(settings, notifier);

        // Case 1: 1024 bytes for file 1 and 100 bytes for file 2

        String key = "test-key";
        CacheFile cacheFile = createCacheFile(key, 1024, bucket, manager);

        // Adding a block to the cache file
        CacheBlock block = new MemoryBlock(0, 1023, stateFolder, pool, cacheFile, usageStats);
        addCacheBlock(block, cacheFile, usageStats);

        // Create a second file
        String key2 = "test-key2";
        CacheFile cacheFile2 = createCacheFile(key2, 100, bucket, manager);
        // Adding a block to the cache file
        CacheBlock block2 = new MemoryBlock(0, 99, stateFolder, pool, cacheFile2, usageStats);
        addCacheBlock(block2, cacheFile2, usageStats);

        EvictionTriggerPolicy triggerPolicy = EvictionTriggerPolicy.getInstance(configuration);

        // Let's assume that we have a disk tier with 1500 bytes
        UsageStats downgradeStats = new UsageStats(1500);

        // Access file 2, 3 times
        for (int i = 0; i < 3; i++) {
            simulateAccess(cacheFile2, policy);
        }
        // File 1 must be chosen for eviction as it is the least frequently used
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile);

        // Access file 1, 4 times
        for (int i = 0; i < 4; i++) {
            simulateAccess(cacheFile, policy);
        }
        // Now, file 2 must be chosen for eviction as it is the least frequently used
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile2);

        // Access file 2 , 2 more times to make sure file 1 is the one that gets evicted
        simulateAccess(cacheFile2, policy);
        simulateAccess(cacheFile2, policy);
        // Finally, file 1 must be chosen for eviction as it is the least frequently used
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile);

        // Have LFU eviction policy start evicting
        List<CacheFile> evictedFiles = new ArrayList<>();
        while (triggerPolicy.triggerEviction(usageStats, downgradeStats, StoreOptionType.MEMORY_ONLY)) {
            simulateEviction(policy, manager, usageStats, evictedFiles);
        }

        // Firstly, only file 1 must be evicted as it is the least frequently used
        assert evictedFiles.size() == 1;
        assert evictedFiles.get(0).equals(cacheFile);

        // Reset the policy
        policy.reset();

        // Case 2: 100 bytes for file 1 and 1024 bytes for file 2

        usageStats.decrement(1124, 1124);
        MemoryManager manager2 = new MemoryManager(settings, notifier);

        // File 1
        CacheFile cacheFile3 = createCacheFile(key, 100, bucket, manager2);
        addCacheBlock(block2, cacheFile3, usageStats);

        // File 2
        CacheFile cacheFile4 = createCacheFile(key2, 1024, bucket, manager2);
        addCacheBlock(block, cacheFile4, usageStats);

        // Access file 2, 3 times to make sure it will be the one evicted
        for (int i = 0; i < 3; i++) {
            simulateAccess(cacheFile4, policy);
        }
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile3);

        // Have LFU eviction policy start evicting
        evictedFiles = new ArrayList<>();
        while (triggerPolicy.triggerEviction(usageStats, downgradeStats, StoreOptionType.MEMORY_ONLY)) {
            simulateEviction(policy, manager2, usageStats, evictedFiles);
        }

        // Now, file 1 must be evicted as it is the least frequently used, but
        // file 2 must also be evicted to deactivate the eviction
        assert evictedFiles.size() == 2;
        assert evictedFiles.get(0).equals(cacheFile3);
        assert evictedFiles.get(1).equals(cacheFile4);

        System.out.println("EvictionItemLFUTest.testEvict() passed");
    }

    private CacheFile createCacheFile(String key, int size, String bucket, MemoryManager manager) throws IOException {
        CacheFile cacheFile = manager.create(bucket, key, size);
        manager.put(bucket, key, cacheFile);
        return cacheFile;
    }

    private void addCacheBlock(CacheBlock block, CacheFile cacheFile, UsageStats usageStats) {
        cacheFile.addIncompleteBlock(block);
        cacheFile.makeBlockVisible(block);
        cacheFile.stateComplete();
        usageStats.increment(block.getSize());
    }

    private void simulateAccess(CacheFile cacheFile, EvictionItemPolicy policy) {
        policy.onItemAccess(cacheFile, StoreOptionType.MEMORY_ONLY);
    }

    private void simulateEviction(EvictionItemPolicy policy, MemoryManager manager,
                                  UsageStats usageStats, List<CacheFile> evictedFiles) {
        CacheFile evictedFile = policy.getItemToEvict(StoreOptionType.MEMORY_ONLY);
        evictedFiles.add(evictedFile);
        manager.delete(evictedFile.getBucket(), evictedFile.getKey());
        manager.evict(evictedFile);
        policy.onItemDelete(evictedFile, StoreOptionType.MEMORY_ONLY);
        usageStats.decrement(evictedFile.getTotalSize(), evictedFile.getActualSize());
    }
}
