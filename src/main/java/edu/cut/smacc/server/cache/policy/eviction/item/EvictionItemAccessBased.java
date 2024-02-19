package edu.cut.smacc.server.cache.policy.eviction.item;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.disk.DiskFile;
import edu.cut.smacc.server.cache.memory.MemoryFile;
import edu.cut.smacc.utils.collections.AccessBasedList;

/**
 * This abstract class is used to implement the common operations
 * of the eviction policies that are based on the access of the files
 * like LRU and MRU
 */
public abstract class EvictionItemAccessBased implements EvictionItemPolicy {

    protected AccessBasedList<MemoryFile> memAbList;
    protected AccessBasedList<DiskFile> diskAbList;

    @Override
    public void initialize(Configuration conf) {
        memAbList = new AccessBasedList<>();
        diskAbList = new AccessBasedList<>();
    }

    @Override
    public void onItemAdd(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY)
            diskAbList.add((DiskFile) file);
        else if (tier == StoreOptionType.MEMORY_ONLY)
            memAbList.add((MemoryFile) file);
    }

    @Override
    public void onItemNotAdded(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemAccess(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY)
            diskAbList.accessItem((DiskFile) file);
        else if (tier == StoreOptionType.MEMORY_ONLY)
            memAbList.accessItem((MemoryFile) file);
    }

    @Override
    public void onItemUpdate(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY)
            diskAbList.accessItem((DiskFile) file);
        else if (tier == StoreOptionType.MEMORY_ONLY)
            memAbList.accessItem((MemoryFile) file);
    }

    @Override
    public void onItemDelete(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY)
            diskAbList.deleteItem((DiskFile) file);
        else if (tier == StoreOptionType.MEMORY_ONLY)
            memAbList.deleteItem((MemoryFile) file);
    }

    @Override
    public void reset() {
        memAbList.clear();
        diskAbList.clear();
    }

}
