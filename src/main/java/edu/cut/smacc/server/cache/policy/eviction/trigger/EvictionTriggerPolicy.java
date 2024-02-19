package edu.cut.smacc.server.cache.policy.eviction.trigger;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.utils.ReflectionUtils;

/**
 * Interface for policies on when to evict
 */
public interface EvictionTriggerPolicy {

    void initialize(Configuration conf);

    /**
     * Check if eviction should be triggered
     * @param cacheStats the usage stats of the tier
     * @param downgradeStats the usage stats of the tier below
     * @param tier the tier to check
     * @return true if eviction is triggered
     */
    boolean triggerEviction(UsageStats cacheStats, UsageStats downgradeStats, StoreOptionType tier);

    static EvictionTriggerPolicy getInstance(Configuration conf) {
        Class<? extends EvictionTriggerPolicy> evictionTriggerClass = conf.getClass(
                ServerConfigurations.EVICTION_TRIGGER_POLICY_KEY,
                ServerConfigurations.EVICTION_TRIGGER_POLICY_DEFAULT,
                EvictionTriggerPolicy.class);
        EvictionTriggerPolicy evictionTriggerPolicy = ReflectionUtils.newInstance(evictionTriggerClass);
        evictionTriggerPolicy.initialize(conf);
        return evictionTriggerPolicy;
    }

}
