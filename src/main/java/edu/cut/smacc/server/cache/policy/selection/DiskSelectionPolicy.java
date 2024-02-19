package edu.cut.smacc.server.cache.policy.selection;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.utils.ReflectionUtils;

import java.util.HashMap;

/**
 * Interface for the disk selection policy.
 * @author Theodoros Danos
 */
public interface DiskSelectionPolicy {

    /**
     * Initialize the policy with ServerConfigurations
     */
    void initialize(Configuration conf);

    int getDiskIndex(HashMap<Integer, StoreSettings> diskSettings);

    static DiskSelectionPolicy getInstance(Configuration conf) {
        Class<? extends DiskSelectionPolicy> diskSelectionClass = conf.getClass(
                ServerConfigurations.DISK_SELECTION_POLICY_KEY,
                ServerConfigurations.DISK_SELECTION_POLICY_DEFAULT,
                DiskSelectionPolicy.class);

        DiskSelectionPolicy diskSelectionPolicy = ReflectionUtils.newInstance(diskSelectionClass);
        diskSelectionPolicy.initialize(conf);
        return diskSelectionPolicy;
    }

}
