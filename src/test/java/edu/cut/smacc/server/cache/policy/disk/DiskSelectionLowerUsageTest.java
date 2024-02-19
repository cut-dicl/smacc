package edu.cut.smacc.server.cache.policy.disk;

import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.policy.selection.DiskSelectionLowerUsage;
import edu.cut.smacc.server.cache.policy.selection.DiskSelectionPolicy;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class DiskSelectionLowerUsageTest {

    @Test
    void testGetDiskIndex() {
        // Initialize 3 disks
        HashMap<Integer, StoreSettings> diskSettings;
        DiskSelectionPolicy diskSelectionLowerUsage = new DiskSelectionLowerUsage();
        diskSettings = new HashMap<>();
        UsageStats disk1UsageStats = new UsageStats();
        UsageStats disk2UsageStats = new UsageStats();
        UsageStats disk3UsageStats = new UsageStats();
        diskSettings.put(0, new StoreSettings("main_folder", "state_folder", disk1UsageStats));
        diskSettings.put(1, new StoreSettings("main_folder", "state_folder", disk2UsageStats));
        diskSettings.put(2, new StoreSettings("main_folder", "state_folder", disk3UsageStats));

        // Increase usage on disks 1 by 100 and 2 by 200
        disk1UsageStats.increment(100);
        disk2UsageStats.increment(200);
        // The disk with the lowest usage must be selected, which is disk 3 with index 2
        assert diskSelectionLowerUsage.getDiskIndex(diskSettings) == 2;
        // Increase usage on disk 3 by 300
        disk3UsageStats.increment(300);
        // The disk with the lowest usage must be selected, which is disk 1 with index 0
        assert diskSelectionLowerUsage.getDiskIndex(diskSettings) == 0;
        // Increase usage on disk 1 by 400
        disk1UsageStats.increment(400);
        // The disk with the lowest usage must be selected, which is disk 2 with index 1
        assert diskSelectionLowerUsage.getDiskIndex(diskSettings) == 1;

        System.out.println("DiskSelectionLowerUsageTest.testGetDiskIndex() passed!");
    }

}
