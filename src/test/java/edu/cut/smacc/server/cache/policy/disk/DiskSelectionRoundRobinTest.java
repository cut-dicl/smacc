package edu.cut.smacc.server.cache.policy.disk;

import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.policy.selection.DiskSelectionPolicy;
import edu.cut.smacc.server.cache.policy.selection.DiskSelectionRoundRobin;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class DiskSelectionRoundRobinTest {

    @Test
    void testGetDiskIndex() {
        // Initialize 3 disks
        HashMap<Integer, StoreSettings> diskSettings;
        DiskSelectionPolicy diskSelectionRoundRobin = new DiskSelectionRoundRobin();
        diskSettings = new HashMap<>();
        UsageStats disk1UsageStats = new UsageStats();
        UsageStats disk2UsageStats = new UsageStats();
        UsageStats disk3UsageStats = new UsageStats();
        diskSettings.put(0, new StoreSettings("main_folder", "state_folder", disk1UsageStats));
        diskSettings.put(1, new StoreSettings("main_folder", "state_folder", disk2UsageStats));
        diskSettings.put(2, new StoreSettings("main_folder", "state_folder", disk3UsageStats));

        // They must be selected in round-robin fashion
        assert diskSelectionRoundRobin.getDiskIndex(diskSettings) == 0;
        assert diskSelectionRoundRobin.getDiskIndex(diskSettings) == 1;
        assert diskSelectionRoundRobin.getDiskIndex(diskSettings) == 2;
        assert diskSelectionRoundRobin.getDiskIndex(diskSettings) == 0;
        assert diskSelectionRoundRobin.getDiskIndex(diskSettings) == 1;
        assert diskSelectionRoundRobin.getDiskIndex(diskSettings) == 2;

        System.out.println("DiskSelectionRoundRobinTest.testGetDiskIndex() passed!");
    }

}
