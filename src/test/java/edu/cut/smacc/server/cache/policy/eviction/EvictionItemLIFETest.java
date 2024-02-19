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
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemLIFE;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import edu.cut.smacc.server.cache.policy.eviction.trigger.EvictionTriggerPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class EvictionItemLIFETest {

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
        configuration.addProperty(ServerConfigurations.EVICTION_TRIGGER_POLICY_THRESHOLD_KEY, 90);
        configuration.addProperty(ServerConfigurations.EVICTION_ITEM_POLICY_KEY,
                EvictionItemLIFE.class.getName());

        ByteBufferPool pool = new ByteBufferPool(2048);

        // Initialize the eviction policy (LIFE)
        EvictionItemPolicy policy = new EvictionItemLIFE();
        configuration.addProperty(ServerConfigurations.EVICTION_POLICY_WINDOW_BASED_AGING_HOURS_KEY, 1);
        policy.initialize(configuration);
        CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy));
        MemoryManager manager = new MemoryManager(settings, notifier);

        // Case 1: 1000 bytes for file 1, 100 bytes for file 2 and 24 bytes for file 3

        String key = "test-key";
        CacheFile cacheFile = createCacheFile(key, 1000, bucket, manager);
        cacheFile.setLastModified(Calendar.getInstance().getTime().getTime());

        // Adding a block to the cache file
        CacheBlock block = new MemoryBlock(0, 1023, stateFolder, pool, cacheFile, usageStats);
        addCacheBlock(block, cacheFile, usageStats);

        // Create a second file
        String key2 = "test-key2";
        CacheFile cacheFile2 = createCacheFile(key2, 100, bucket, manager);
        cacheFile2.setLastModified(Calendar.getInstance().getTime().getTime());

        // Adding a block to the cache file
        CacheBlock block2 = new MemoryBlock(0, 99, stateFolder, pool, cacheFile2, usageStats);
        addCacheBlock(block2, cacheFile2, usageStats);

        // Create a third file
        String key3 = "test-key3";
        CacheFile cacheFile3 = createCacheFile(key3, 24, bucket, manager);
        cacheFile3.setLastModified(Calendar.getInstance().getTime().getTime());

        // Adding a block to the cache file
        CacheBlock block3 = new MemoryBlock(0, 23, stateFolder, pool, cacheFile3, usageStats);
        addCacheBlock(block3, cacheFile3, usageStats);

        EvictionTriggerPolicy triggerPolicy = EvictionTriggerPolicy.getInstance(configuration);

        // Let's assume that we have a disk tier with 1500 bytes
        UsageStats downgradeStats = new UsageStats(1500);

        // File 1 must be chosen for eviction
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile);
        // Have LIFE eviction policy start evicting
        List<CacheFile> evictedFiles = new ArrayList<>();
        while (triggerPolicy.triggerEviction(usageStats, downgradeStats, StoreOptionType.MEMORY_ONLY)) {
            simulateEviction(policy, manager, usageStats, evictedFiles);
        }

        // Firstly, only file 1 must be evicted
        assert evictedFiles.size() == 1;
        assert evictedFiles.get(0).equals(cacheFile);

        // Access file 3
        simulateAccess(cacheFile3, policy);
        // File 2 must be chosen for eviction
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile2);

        // Add a cache block to file 3
        CacheBlock block4 = new MemoryBlock(0, 999, stateFolder, pool, cacheFile3, usageStats);
        addCacheBlock(block4, cacheFile3, usageStats);
        policy.onItemUpdate(cacheFile3, StoreOptionType.MEMORY_ONLY);

        // Access file 2
        simulateAccess(cacheFile2, policy);
        // File 3 must be chosen for eviction
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile3);

        // Let LIFE eviction policy start evicting
        evictedFiles.clear();
        while (triggerPolicy.triggerEviction(usageStats, downgradeStats, StoreOptionType.MEMORY_ONLY)) {
            simulateEviction(policy, manager, usageStats, evictedFiles);
        }

        // File 3 must be the only one evicted
        assert evictedFiles.size() == 1;
        assert evictedFiles.get(0).equals(cacheFile3);

        System.out.println("EvictionItemLIFETest.testEvict() passed");
    }

    private CacheFile createCacheFile(String key, int size, String bucket, MemoryManager manager) throws IOException {
        CacheFile cacheFile = manager.create(bucket, key, size);
        cacheFile.setLastModified(Calendar.getInstance().getTime().getTime());
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
