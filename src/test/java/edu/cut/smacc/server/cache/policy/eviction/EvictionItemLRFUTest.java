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
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemLRFU;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import edu.cut.smacc.server.cache.policy.eviction.trigger.EvictionTriggerPolicy;
import edu.cut.smacc.utils.collections.WeightedLRFUNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EvictionItemLRFUTest {

    @Test
    void testEvict() throws IOException, InterruptedException {
        // Creating a memory file with no parent
        String bucket = "test-bucket";
        String mainFolder = "";
        String stateFolder = "";
        UsageStats usageStats = new UsageStats(1124);
        StoreSettings settings = new StoreSettings(mainFolder, stateFolder, usageStats);

        // Put more emphasis on recency
        WeightedLRFUNode.BIAS = 10000000;
        // Trigger evictions at 90%
        Configuration configuration = new Configuration();
        configuration.addProperty(ServerConfigurations.EVICTION_TRIGGER_POLICY_KEY,
                ThresholdPercentageTrigger.class.getName());
        configuration.addProperty(ServerConfigurations.EVICTION_TRIGGER_POLICY_THRESHOLD_KEY,
                90);
        configuration.addProperty(ServerConfigurations.EVICTION_ITEM_POLICY_KEY,
                EvictionItemLRFU.class.getName());

        ByteBufferPool pool = new ByteBufferPool(2048);

        // Initialize the eviction policy (LRFU)
        EvictionItemPolicy policy = new EvictionItemLRFU();
        policy.initialize(configuration);
        MemoryManager manager = new MemoryManager(settings, CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy)));

        // Case 1: 1024 bytes for file 1 and 100 bytes for file 2

        String key = "test-key";
        CacheFile cacheFile = createCacheFile(key, 1024, bucket, manager);
        // Add a block to the cache file
        CacheBlock block = new MemoryBlock(0, 1023, stateFolder, pool, cacheFile, usageStats);
        addCacheBlock(block, cacheFile, usageStats);

        // Create a second file
        String key2 = "test-key2";
        CacheFile cacheFile2 = createCacheFile(key2, 100, bucket, manager);
        // Add a block to the cache file
        CacheBlock block2 = new MemoryBlock(0, 99, stateFolder, pool, cacheFile2, usageStats);
        addCacheBlock(block2, cacheFile2, usageStats);

        EvictionTriggerPolicy triggerPolicy = EvictionTriggerPolicy.getInstance(configuration);

        // Let's assume that we have a disk tier with 1500 bytes
        UsageStats downgradeStats = new UsageStats(1500);

        // Access file 1 , 10 times
        for (int i = 0; i < 10; i++) {
            simulateAccess(cacheFile, policy);
            Thread.sleep(10);
        }
        // File 2 must be chosen for eviction for both frequency and recency
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile2);

        // Access file 2, 10 times
        for (int i = 0; i < 10; i++) {
            simulateAccess(cacheFile2, policy);
        }
        // Even though the 2 files have the same frequency of accesses, due to recency, file 1 must be chosen for eviction
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile);

        // Have LRFU eviction policy start evicting
        List<CacheFile> evictedFiles = new ArrayList<>();
        while (triggerPolicy.triggerEviction(usageStats, downgradeStats, StoreOptionType.MEMORY_ONLY)) {
            simulateEviction(policy, manager, usageStats, evictedFiles);
        }

        assert evictedFiles.size() == 1;
        assert evictedFiles.get(0).equals(cacheFile);

        // Reset the policy
        policy.reset();

        // Case 2: 100 bytes for file 1 and 1024 bytes for file 2

        usageStats.decrement(1124, 1124);
        MemoryManager manager2 = new MemoryManager(settings, CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy)));

        // File 1
        CacheFile cacheFile3 = createCacheFile(key, 100, bucket, manager2);
        addCacheBlock(block2, cacheFile3, usageStats);

        // File 2
        CacheFile cacheFile4 = createCacheFile(key2, 1024, bucket, manager2);
        addCacheBlock(block, cacheFile4, usageStats);

        // Access file 1, 3 times
        for (int i = 0; i < 3; i++) {
            simulateAccess(cacheFile3, policy);
            Thread.sleep(5);
        }
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile4);

        // Access file 2, 3 times
        for (int i = 0; i < 3; i++) {
            simulateAccess(cacheFile4, policy);
        }
        // Draw on frequency, 2 wins on recency
        assert policy.getItemToEvict(StoreOptionType.MEMORY_ONLY).equals(cacheFile3);

        // Have LRFU eviction policy start evicting
        evictedFiles = new ArrayList<>();
        while (triggerPolicy.triggerEviction(usageStats, downgradeStats, StoreOptionType.MEMORY_ONLY)) {
            simulateEviction(policy, manager2, usageStats, evictedFiles);
        }

        // Now, file 1 must be evicted as it is the least frequently used, but
        // file 2 must also be evicted to deactivate the eviction
        assert evictedFiles.size() == 2;
        assert evictedFiles.get(0).equals(cacheFile3);
        assert evictedFiles.get(1).equals(cacheFile4);

        System.out.println("EvictionItemLRFUTest.testEvict() passed");
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
