package edu.cut.smacc.server.cache;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.disk.DiskManager;
import edu.cut.smacc.server.cache.memory.MemoryManager;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class CacheFileHelperTest {

    @Test
    void testHandleDelete() throws IOException {
        // Have 2 cache policies
        Configuration configuration = new Configuration();
        EvictionItemPolicy policy = EvictionItemPolicy.getInstance(configuration);

        // Create a CachePolicyNotifier and the files
        CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy));

        MemoryManager memoryManager = (MemoryManager) getMemoryManager(notifier);
        DiskManager diskManager = (DiskManager) getDiskManager(notifier);

        CacheFile memFile = mockMemoryFileCreation(memoryManager);
        CacheFile diskFile = mockDiskFileCreation(diskManager);

        assert memoryManager.getFile("test", "test") == memFile;
        assert diskManager.getFile("test", "test") == diskFile;
        CacheFileHelper.handleDelete("test", "test", memoryManager);
        CacheFileHelper.handleDelete("test", "test", diskManager);
        assert memoryManager.getFile("test", "test") == null;
        assert diskManager.getFile("test", "test") == null;

        System.out.println("CacheFileHelperTest.testHandleDelete passed" );
    }


    private CacheManager getMemoryManager(CachePolicyNotifier notifier) {
        UsageStats usageStats = new UsageStats(1124);
        StoreSettings settings = new StoreSettings("test", "test", usageStats);
        return new MemoryManager(settings, notifier);
    }

    private CacheManager getDiskManager(CachePolicyNotifier notifier) {
        StoreSettings diskSettings = new StoreSettings("test", "test", new UsageStats());
        HashMap<Integer, StoreSettings> diskSettingsMap = new HashMap<>();
        diskSettingsMap.put(0, diskSettings);
        Configuration diskConfig = new Configuration();
        return new DiskManager(diskSettingsMap, diskConfig, notifier);
    }

    private CacheFile mockDiskFileCreation(DiskManager diskManager) throws IOException {
        CacheFile cFile = diskManager.create("test", "test");
        diskManager.put(cFile.getBucket(), cFile.getKey(), cFile);
        return cFile;
    }

    private CacheFile mockMemoryFileCreation(MemoryManager manager) {
        CacheFile cFile = manager.create("test", "test");
        manager.put(cFile.getBucket(), cFile.getKey(), cFile);
        return cFile;
    }

}
