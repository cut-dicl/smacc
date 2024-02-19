package edu.cut.smacc.server.cache.common;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.io.ByteBufferPool;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.memory.MemoryBlock;
import edu.cut.smacc.server.cache.memory.MemoryManager;
import edu.cut.smacc.server.cache.policy.CachePolicy;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class CacheFileTest {

    @Test
    void testMemoryCacheFile() throws IOException {
        // Creating a memory file with no parent
        String bucket = "test-bucket";
        String key = "test-key";
        String mainFolder = "";
        String stateFolder = "";
        UsageStats usageStats = new UsageStats(1124);
        StoreSettings settings = new StoreSettings(mainFolder, stateFolder, usageStats);
        CachePolicy policy = EvictionItemPolicy.getInstance(new Configuration());
        CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy));
        MemoryManager manager = new MemoryManager(settings, notifier);
        CacheFile cacheFile = manager.create(bucket, key, 1024);

        manager.put(bucket, key, cacheFile);

        // Testing the cache file
        assert cacheFile != null;
        assert cacheFile.getBucket().equals(bucket);
        assert cacheFile.getKey().equals(key);
        assert cacheFile.getSettings() == settings;
        assert cacheFile.type() == CacheType.MEMORY_FILE;
        assert cacheFile.isPartialFile();
        assert !cacheFile.isFullFile();
        assert !cacheFile.isObsolete();
        assert manager.getCacheFiles().get(0) == cacheFile;

        // Adding a block to the cache file
        ByteBufferPool pool = new ByteBufferPool(2048);
        CacheBlock block = new MemoryBlock(0, 1023, stateFolder, pool, cacheFile, usageStats);
        cacheFile.addIncompleteBlock(block);
        assert cacheFile.getTotalSize() == 1024;
        cacheFile.makeBlockVisible(block);
        cacheFile.stateComplete();
        assert cacheFile.getSize() == cacheFile.getSize();
        assert cacheFile.getCacheBlocks().size() == 1;
        assert cacheFile.getCacheBlocks().get(0) == block;
        assert cacheFile.isFullFile();
        usageStats.increment(1024);

        // Create a second file
        String key2 = "test-key2";
        CacheFile cacheFile2 = manager.create(bucket, key2, 100);
        manager.put(bucket, key2, cacheFile2);
        CacheBlock block2 = new MemoryBlock(0, 99, stateFolder, pool, cacheFile2, usageStats);
        cacheFile2.addIncompleteBlock(block2);
        cacheFile2.makeBlockVisible(block2);
        cacheFile2.stateComplete();
        usageStats.increment(100);

        assert manager.getReportedUsage() == cacheFile.getSize() + cacheFile2.getSize();
        assert usageStats.getActualUsage() == usageStats.getReportedUsage();

        // Delete the first file
        manager.delete(bucket, key);
        usageStats.decrement(1024, 1024);
        assert manager.getReportedUsage() == cacheFile2.getSize();
        assert usageStats.getActualUsage() == usageStats.getReportedUsage();

        // Delete the second file
        manager.delete(bucket, key2);
        usageStats.decrement(100, 100);
        assert manager.getReportedUsage() == 0;
        assert usageStats.getActualUsage() == usageStats.getReportedUsage();

        assert manager.getCacheFiles().size() == 0;

        System.out.println("Memory cache file test passed");
    }

}
