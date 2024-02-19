package edu.cut.smacc.server.cache.policy.selection;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.StoreSettings;

import java.util.HashMap;

public class DiskSelectionRoundRobin implements DiskSelectionPolicy {

    private int selectionIndex;

    public DiskSelectionRoundRobin() {
        this.selectionIndex = 0;
    }

    @Override
    public void initialize(Configuration conf) {
        this.selectionIndex = 0;
    }

    @Override
    public int getDiskIndex(HashMap<Integer, StoreSettings> diskSettings) {
        if (selectionIndex == diskSettings.size()) {
            selectionIndex = 1;
            return 0;
        }
        int currentSelectionIndex = selectionIndex;
        selectionIndex++;
        return currentSelectionIndex;
    }
}
