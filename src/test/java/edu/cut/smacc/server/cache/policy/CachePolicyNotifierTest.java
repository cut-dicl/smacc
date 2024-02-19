package edu.cut.smacc.server.cache.policy;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.disk.DiskManager;
import edu.cut.smacc.server.cache.memory.MemoryManager;
import edu.cut.smacc.server.cache.policy.admission.AdmissionAlways;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemEXD;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class CachePolicyNotifierTest {

  // Mock initializing a CachePolicyNotifier
  @Test
  void testCachePolicyNotifier() {
    // Have 2 cache policies
    CachePolicy policy1 = new AdmissionAlways();
    CachePolicy policy2 = new EvictionItemEXD();
    ArrayList<CachePolicy> policies = new ArrayList<>();

    policies.add(policy1);
    policies.add(policy2);

    // Create a CachePolicyNotifier
    CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(policies);

    assert notifier.getCachePolicies() == policies;

    System.out.println("CachePolicyNotifierTest.testCachePolicyNotifier() passed");
  }

    @Test
    void testNotifyItemOperations() throws IOException {
        // Have 2 cache policies
        Configuration configuration = new Configuration();
        EvictionItemPolicy policy1 = EvictionItemPolicy.getInstance(configuration);
        configuration.addProperty(ServerConfigurations.EVICTION_ITEM_POLICY_KEY, ServerConfigurations.EVICTION_ITEM_POLICY_ALTERNATIVES.get(2));
        EvictionItemPolicy policy2 = EvictionItemPolicy.getInstance(configuration);
        ArrayList<CachePolicy> policies = new ArrayList<>();

        policies.add(policy1);
        policies.add(policy2);

        // Create a CachePolicyNotifier and the files
        CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(policies);
        CacheFile file1 = mockMemoryFileCreation(notifier);
        CacheFile file2 = mockDiskFileCreation(notifier);

        // All the assertions here should be null, since the items are not yet added to the cache
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == null;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == null;
        // Notify the disk and memory file accesses
        notifier.notifyItemAccess(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemAccess(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == null;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == null;
        // Notify updates
        notifier.notifyItemUpdate(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemUpdate(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == null;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == null;
        // Notify not added operations
        notifier.notifyItemNotAdded(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemNotAdded(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == null;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == null;
        // Notify deletions
        notifier.notifyItemDeletion(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemDeletion(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == null;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == null;


        // Add them and access them, and everything should return the corresponding file, until deletion
        notifier.notifyItemAddition(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemAddition(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == file1;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == file2;
        // Notify the disk and memory file accesses
        notifier.notifyItemAccess(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemAccess(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == file1;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == file2;
        // Notify updates
        notifier.notifyItemUpdate(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemUpdate(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == file1;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == file2;
        // Notify not added operations
        notifier.notifyItemNotAdded(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemNotAdded(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == file1;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == file2;
        // Notify deletions
        notifier.notifyItemDeletion(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemDeletion(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == null;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == null;
        // Add them again and reset the policies
        notifier.notifyItemAddition(file1, StoreOptionType.MEMORY_ONLY);
        notifier.notifyItemAddition(file2, StoreOptionType.DISK_ONLY);
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == file1;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == file2;
        notifier.notifyPolicyReset();
        assert policy1.getItemToEvict(StoreOptionType.MEMORY_ONLY) == null;
        assert policy2.getItemToEvict(StoreOptionType.DISK_ONLY) == null;

        System.out.println("CachePolicyNotifierTest.testNotifyOperations() passed");
    }

    private CacheFile mockDiskFileCreation(CachePolicyNotifier notifier) throws IOException {
        StoreSettings diskSettings = new StoreSettings("test", "test", new UsageStats());
        HashMap<Integer, StoreSettings> diskSettingsMap = new HashMap<>();
        diskSettingsMap.put(0, diskSettings);
        Configuration diskConfig = new Configuration();
        DiskManager diskManager = new DiskManager(diskSettingsMap, diskConfig, notifier);
        return diskManager.create("test", "test");
    }

    private CacheFile mockMemoryFileCreation(CachePolicyNotifier notifier) {
        UsageStats usageStats = new UsageStats(1124);
        StoreSettings settings = new StoreSettings("test", "test", usageStats);
        MemoryManager manager = new MemoryManager(settings, notifier);
        return manager.create("test", "test");
    }

}
