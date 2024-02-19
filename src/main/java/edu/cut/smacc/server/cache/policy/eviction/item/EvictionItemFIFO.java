package edu.cut.smacc.server.cache.policy.eviction.item;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of algorithm FIFO as an eviction policy
 *
 * @author Michail Boronikolas
 */
public class EvictionItemFIFO implements EvictionItemPolicy {

    private List<CacheFile> memFileList;
    private List<CacheFile> diskFileList;

    @Override
    public void initialize(Configuration conf) {
        memFileList = new ArrayList<>();
        diskFileList = new ArrayList<>();
    }

    @Override
    public CacheFile getItemToEvict(StoreOptionType evictionTier) {
        if (evictionTier.equals(StoreOptionType.MEMORY_ONLY)) {
            if (memFileList.isEmpty()) {
                return null;
            }
            return memFileList.get(0);
        } else if (evictionTier.equals(StoreOptionType.DISK_ONLY)) {
            if (diskFileList.isEmpty()) {
                return null;
            }
            return diskFileList.get(0);
        } else {
            return null;
        }
    }

    @Override
    public void onItemAdd(CacheFile file, StoreOptionType tier) {
        if (tier.equals(StoreOptionType.MEMORY_ONLY))
            memFileList.add(file);
        else if (tier.equals(StoreOptionType.DISK_ONLY))
            diskFileList.add(file);
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
        if (tier.equals(StoreOptionType.MEMORY_ONLY))
            memFileList.remove(file);
        else if (tier.equals(StoreOptionType.DISK_ONLY))
            diskFileList.remove(file);
    }

    @Override
    public void reset() {
        memFileList.clear();
        diskFileList.clear();
    }
}
