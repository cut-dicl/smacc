package edu.cut.smacc.server.cache.policy.selection;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.StoreSettings;

import java.util.HashMap;

public class DiskSelectionLowerUsage implements DiskSelectionPolicy {

    public DiskSelectionLowerUsage() {
        // Do nothing
    }

    @Override
    public void initialize(Configuration conf) {
        // Do nothing
    }

    @Override
    public int getDiskIndex(HashMap<Integer, StoreSettings> diskSettings) {
        long minUsage = Long.MAX_VALUE;
        int minIndex = 0;
        for (int i = 0; i < diskSettings.size(); i++) {
            long diskUsage = diskSettings.get(i).getStats().getActualUsage();
            if (diskUsage < minUsage) {
                minUsage = diskUsage;
                minIndex = i;
            }
        }
        return minIndex;
    }
}
