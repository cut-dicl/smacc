package edu.cut.smacc.server.statistics;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.disk.DiskManager;
import edu.cut.smacc.server.cache.memory.MemoryManager;
import edu.cut.smacc.server.cache.policy.CachePolicy;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemLRU;
import edu.cut.smacc.server.statistics.type.StatisticType;
import edu.cut.smacc.server.statistics.type.performance.PerformanceStatistics;
import edu.cut.smacc.server.statistics.type.general.TierGeneralStatistics;
import edu.cut.smacc.utils.BasicGlobalTimer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class StatisticsManagerTest {

    @Test
    void testInitializeStatisticsManager() {
        Configuration configuration = new Configuration();
        StoreSettings diskSettings = new StoreSettings("test", "test", new UsageStats());
        HashMap<Integer, StoreSettings> diskSettingsMap = new HashMap<>();
        diskSettingsMap.put(0, diskSettings);
        CachePolicy policy = new EvictionItemLRU();
        CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy));
        DiskManager diskManager = new DiskManager(diskSettingsMap, configuration, notifier);

        UsageStats usageStats = new UsageStats(1124);
        StoreSettings settings = new StoreSettings("test", "test", usageStats);
        MemoryManager memoryManager = new MemoryManager(settings, notifier);

        PerformanceStatistics storageStatistics = new PerformanceStatistics();

        // Initialize the Statistics Manager
        StatisticsManager manager = new StatisticsManager(configuration, memoryManager.getTierStatistics(),
                diskManager.getTierStatistics(), storageStatistics, "statistics_test.tsv");
        // It's not time to output, since no operations have been performed
        assert !manager.outputStatistics();

        System.out.println("StatisticsManagerTest.testInitializeStatisticsManager() passed");
    }

    @Test
    void testStatisticsManagerWithOnlyOneTierAvailable() {
        // We only have a memory tier available
        Configuration configuration = new Configuration();
        UsageStats usageStats = new UsageStats(1124);
        StoreSettings settings = new StoreSettings("test", "test", usageStats);
        CachePolicy policy = new EvictionItemLRU();
        CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy));
        MemoryManager memoryManager = new MemoryManager(settings, notifier);

        PerformanceStatistics storageStatistics = new PerformanceStatistics();

        // Initialize the Statistics Manager
        StatisticsManager manager = new StatisticsManager(configuration, memoryManager.getTierStatistics(), null,
                storageStatistics, "statistics_test.tsv");
        // It's not time to output, since no operations have been performed
        assert !manager.outputStatistics();
        // Add some statistics that will be reset after being outputted
        memoryManager.getTierStatistics().incrementStat(StatisticType.CACHE_FILES);
        assert memoryManager.getTierStatistics().getStat(StatisticType.CACHE_FILES).longValue() == 1;
        // outputStatistics() method is called after each operation, so now it should be time to output
        assert manager.outputStatistics();
        // The statistics should have been reset
        assert memoryManager.getTierStatistics().getStat(StatisticType.CACHE_FILES).longValue() == 0;

        // Now we only have a disk tier available
        StoreSettings diskSettings = new StoreSettings("test", "test", new UsageStats());
        HashMap<Integer, StoreSettings> diskSettingsMap = new HashMap<>();
        diskSettingsMap.put(0, diskSettings);
        DiskManager diskManager = new DiskManager(diskSettingsMap, configuration, notifier);
        manager = new StatisticsManager(configuration, null, diskManager.getTierStatistics(),
                storageStatistics, "statistics_test.tsv");
        // It's not time to output, since no operations have been performed
        assert !manager.outputStatistics();
        // Now it is time to output
        assert manager.outputStatistics();

        deleteFile();
        System.out.println("StatisticsManagerTest.testStatisticsManagerWithOnlyOneTierAvailable() passed");
    }

    @Test
    void testCollectStatisticsBeforeOutput() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        BasicGlobalTimer.startTimer();

        Configuration configuration = new Configuration();
        StoreSettings diskSettings = new StoreSettings("test", "test", new UsageStats());
        HashMap<Integer, StoreSettings> diskSettingsMap = new HashMap<>();
        diskSettingsMap.put(0, diskSettings);
        CachePolicy policy = new EvictionItemLRU();
        CachePolicyNotifier notifier = CachePolicyNotifier.createNotifierFromPoliciesList(List.of(policy));
        DiskManager diskManager = new DiskManager(diskSettingsMap, configuration, notifier);

        UsageStats usageStats = new UsageStats(1124);
        StoreSettings settings = new StoreSettings("test", "test", usageStats);
        MemoryManager memoryManager = new MemoryManager(settings, notifier);

        PerformanceStatistics storageStatistics = new PerformanceStatistics();

        // Initialize the Statistics Manager
        StatisticsManager manager = new StatisticsManager(configuration, memoryManager.getTierStatistics(),
                diskManager.getTierStatistics(), storageStatistics, "statistics_test.tsv");
        // It's not time to output, since no operations have been performed
        assert !manager.outputStatistics();

        // Playing with memory statistics
        TierGeneralStatistics memoryStatistics = memoryManager.getTierStatistics();
        // Increment PUT, GET, DELETE count by 1, bytes by 1024 and time by 5
        memoryStatistics.incrementStat(StatisticType.PUT_COUNT);
        memoryStatistics.incrementStat(StatisticType.GET_COUNT);
        memoryStatistics.incrementStat(StatisticType.DEL_COUNT);
        memoryStatistics.incrementStatBy(StatisticType.PUT_BYTES, 1024);
        memoryStatistics.incrementStatBy(StatisticType.GET_BYTES, 1024);
        memoryStatistics.incrementStatBy(StatisticType.DEL_BYTES, 1024);
        memoryStatistics.incrementStatBy(StatisticType.PUT_TIME, 5);
        memoryStatistics.incrementStatBy(StatisticType.GET_TIME, 5);
        memoryStatistics.incrementStatBy(StatisticType.DEL_TIME, 5);

        // Throughput, IOPS and latency are not aggregated yet
        assert memoryStatistics.getStat(StatisticType.PUT_THROUGHPUT).longValue() == 0l;
        assert memoryStatistics.getStat(StatisticType.GET_THROUGHPUT).longValue() == 0l;
        assert memoryStatistics.getStat(StatisticType.DEL_THROUGHPUT).longValue() == 0l;
        assert memoryStatistics.getStat(StatisticType.PUT_IOPS).longValue() == 0l;
        assert memoryStatistics.getStat(StatisticType.GET_IOPS).longValue() == 0l;
        assert memoryStatistics.getStat(StatisticType.DEL_IOPS).longValue() == 0l;

        // Using Java reflection to execute collect without resetting the stats
        Method aggregateOperationStatistics = StatisticsManager.class.getDeclaredMethod("collectStatisticsBeforeOutput");
        // Allow access to the private method
        aggregateOperationStatistics.setAccessible(true);
        // Call the private method
        aggregateOperationStatistics.invoke(manager);
        assert memoryStatistics.getStat(StatisticType.PUT_COUNT).longValue() == 1;
        assert memoryStatistics.getStat(StatisticType.PUT_BYTES).longValue() == 1024;
        assert memoryStatistics.getStat(StatisticType.GET_COUNT).longValue() == 1;
        assert memoryStatistics.getStat(StatisticType.GET_BYTES).longValue() == 1024;
        assert memoryStatistics.getStat(StatisticType.DEL_COUNT).longValue() == 1;
        assert memoryStatistics.getStat(StatisticType.DEL_BYTES).longValue() == 1024;
        assert memoryStatistics.getStat(StatisticType.PUT_TIME).longValue() == 5;
        assert memoryStatistics.getStat(StatisticType.GET_TIME).longValue() == 5;
        assert memoryStatistics.getStat(StatisticType.DEL_TIME).longValue() == 5;
        // Throughput, IOPS and latency are aggregated now
        assert storageStatistics.getStat(StatisticType.PUT_THROUGHPUT).doubleValue() > 0;
        assert storageStatistics.getStat(StatisticType.GET_THROUGHPUT).doubleValue() > 0;
        assert storageStatistics.getStat(StatisticType.DEL_THROUGHPUT).doubleValue() > 0;
        assert storageStatistics.getStat(StatisticType.PUT_IOPS).doubleValue() > 0;
        assert storageStatistics.getStat(StatisticType.GET_IOPS).doubleValue() > 0;
        assert storageStatistics.getStat(StatisticType.DEL_IOPS).doubleValue() > 0;
        // Outputting should reset them
        assert manager.outputStatistics();
        for (StatisticType countBasedStat : StatisticType.listAllPerformanceStats()) {
            assert storageStatistics.getStat(countBasedStat).doubleValue() == 0;
        }

        deleteFile();

        System.out.println("StatisticsManagerTest.testCollectStatisticsBeforeOutput() passed");
    }

    private void deleteFile() {
        try {
            Files.delete(Paths.get("statistics_test.tsv"));
            System.out.println("File deleted successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while deleting the file.");
            e.printStackTrace();
        }
    }

}
