package edu.cut.smacc.server.cache.policy.eviction.trigger;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.common.io.UsageStats;

public class ThresholdPercentageTrigger implements EvictionTriggerPolicy {

    private double percentageThreshold;

    @Override
    public void initialize(Configuration conf) {
        percentageThreshold = conf.getDouble(ServerConfigurations.EVICTION_TRIGGER_POLICY_THRESHOLD_KEY,
                ServerConfigurations.EVICTION_TRIGGER_POLICY_THRESHOLD_DEFAULT);
    }

    @Override
    public boolean triggerEviction(UsageStats cacheStats, UsageStats downgradeStats, StoreOptionType tier) {
        long downgradeMem = downgradeStats == null ? 0 : downgradeStats.getReportedUsage();
        double percentage = (((double) cacheStats.getReportedUsage() - downgradeMem) / cacheStats.getMaxCapacity()) * 100;
        return percentage >= percentageThreshold;
    }

}
