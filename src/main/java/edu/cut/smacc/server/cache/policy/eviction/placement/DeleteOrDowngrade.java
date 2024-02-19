package edu.cut.smacc.server.cache.policy.eviction.placement;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;

public class DeleteOrDowngrade implements EvictionPlacementPolicy {

    private boolean willDowngrade;

    @Override
    public void initialize(Configuration conf) {
        willDowngrade = ServerConfigurations.getEvictionDeleteOrDowngrade();
    }

    @Override
    public boolean downgrade(CacheFile file) {
        return willDowngrade;
    }

    @Override
    public void onItemAdd(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemNotAdded(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemAccess(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemUpdate(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemDelete(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void reset() {
        // Do nothing
    }
}
