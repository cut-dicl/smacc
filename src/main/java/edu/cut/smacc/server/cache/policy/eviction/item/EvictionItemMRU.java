package edu.cut.smacc.server.cache.policy.eviction.item;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;

/**
 * An implementation of algorithm MRU as an eviction policy
 *
 * @author Michail Boronikolas
 */
public class EvictionItemMRU extends EvictionItemAccessBased {

    @Override
    public CacheFile getItemToEvict(StoreOptionType evictionTier) {
        if (evictionTier == StoreOptionType.DISK_ONLY)
            return diskAbList.getMRUItem();
        else if (evictionTier == StoreOptionType.MEMORY_ONLY)
            return memAbList.getMRUItem();
        else
            return null;
    }

}
