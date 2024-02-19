package edu.cut.smacc.server.cache.policy;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;

/**
 * An interface for all data cache policies that can be used for tracking file
 * interactions with the cache
 */
public interface CachePolicy {

    /**
     * Called when a new file is added.
     *
     * @param file the cache file
     */
    void onItemAdd(CacheFile file, StoreOptionType tier);

    /**
     * Called when a file is not added to the cache.
     *
     * @param file the cache file
     */
    void onItemNotAdded(CacheFile file, StoreOptionType tier);

    /**
     * Called when a file is accessed.
     *
     * @param file the cache file
     */
    void onItemAccess(CacheFile file, StoreOptionType tier);

    /**
     * Called when a file is updated.
     *
     * @param file the cache file
     */
    void onItemUpdate(CacheFile file, StoreOptionType tier);

    /**
     * Called when a file is deleted.
     *
     * @param file the cache file
     */
    void onItemDelete(CacheFile file, StoreOptionType tier);

    /**
     * Completely resets the state of the policy
     */
    void reset();

}
