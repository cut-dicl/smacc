package edu.cut.smacc.server.cache.policy.eviction.item;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;

/**
 * An implementation of algorithm LRU as an eviction policy
 *
 * @author Theodoros Danos
 */
public class EvictionItemLRU extends EvictionItemAccessBased {

    @Override
    public CacheFile getItemToEvict(StoreOptionType evictionTier) {
        if (evictionTier == StoreOptionType.DISK_ONLY)
            return diskAbList.getLRUItem();
        else if (evictionTier == StoreOptionType.MEMORY_ONLY)
            return memAbList.getLRUItem();
        return null;
    }

}
