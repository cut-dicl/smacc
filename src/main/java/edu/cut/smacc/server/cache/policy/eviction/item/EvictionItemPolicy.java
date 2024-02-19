package edu.cut.smacc.server.cache.policy.eviction.item;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.policy.CachePolicy;
import edu.cut.smacc.utils.ReflectionUtils;

/**
 * Interface for policies that decide what item to evict,
 * when eviction is triggered.
 */
public interface EvictionItemPolicy extends CachePolicy {

    /**
     * Initialize the policy with ServerConfigurations
     */
    void initialize(Configuration conf);

    /**
     * Get item to evict according to the policy
     * @param evictionTier the tier to evict from
     * @return the item to evict
     */
    CacheFile getItemToEvict(StoreOptionType evictionTier);

    /**
     * Get an instance of the policy
     * @param conf the configuration to use
     * @return the policy instance
     */
    static EvictionItemPolicy getInstance(Configuration conf) {
        Class<? extends EvictionItemPolicy> evictionItemClass = conf.getClass(
                ServerConfigurations.EVICTION_ITEM_POLICY_KEY,
                ServerConfigurations.EVICTION_ITEM_POLICY_DEFAULT,
                EvictionItemPolicy.class);

        EvictionItemPolicy evictionItemPolicy = ReflectionUtils.newInstance(evictionItemClass);
        evictionItemPolicy.initialize(conf);
        return evictionItemPolicy;
    }

}
