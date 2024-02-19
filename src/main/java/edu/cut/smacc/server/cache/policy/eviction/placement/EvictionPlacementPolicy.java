package edu.cut.smacc.server.cache.policy.eviction.placement;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.policy.CachePolicy;
import edu.cut.smacc.utils.ReflectionUtils;

/**
 * Interface for policies on where to evict
 */
public interface EvictionPlacementPolicy extends CachePolicy {

    void initialize(Configuration conf);

    /**
     * Downgrade the file to the next lower tier, or delete it.
     * @param file the file to downgrade
     * @return true if the file should be downgraded, false if it should be deleted
     */
    boolean downgrade(CacheFile file);

    static EvictionPlacementPolicy getInstance(Configuration conf) {
        Class<? extends EvictionPlacementPolicy> evictionPlacementClass = conf.getClass(
                ServerConfigurations.EVICTION_PLACEMENT_POLICY_KEY,
                ServerConfigurations.EVICTION_PLACEMENT_POLICY_DEFAULT,
                EvictionPlacementPolicy.class);
        EvictionPlacementPolicy evictionPlacementPolicy = ReflectionUtils.newInstance(evictionPlacementClass);
        evictionPlacementPolicy.initialize(conf);
        return evictionPlacementPolicy;
    }
}
