package edu.cut.smacc.server.cache.policy.eviction.item;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.utils.collections.SortedWeightedTree;
import edu.cut.smacc.utils.collections.WeightedLFUNode;

/**
 * This policy implements LFU (Least Frequently Used) to select a cache item for eviction
 */
public class EvictionItemLFU implements EvictionItemPolicy {

    private SortedWeightedTree<WeightedLFUNode<CacheFile>, CacheFile> diskTree;
    private SortedWeightedTree<WeightedLFUNode<CacheFile>, CacheFile> memTree;

    @Override
    public void initialize(Configuration conf) {
        diskTree = new SortedWeightedTree<>();
        memTree = new SortedWeightedTree<>();
    }

    @Override
    public CacheFile getItemToEvict(StoreOptionType evictionTier) {
        if (evictionTier == StoreOptionType.DISK_ONLY)
            return diskTree.getMinWeightItem();
        else if (evictionTier == StoreOptionType.MEMORY_ONLY)
            return memTree.getMinWeightItem();

        return null;
    }

    @Override
    public void onItemAdd(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY)
            diskTree.addNode(new WeightedLFUNode<>(file));
        else if (tier == StoreOptionType.MEMORY_ONLY)
            memTree.addNode(new WeightedLFUNode<>(file));
    }

    @Override
    public void onItemNotAdded(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemAccess(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY)
            diskTree.updateNode(file);
        else if (tier == StoreOptionType.MEMORY_ONLY)
            memTree.updateNode(file);
    }

    @Override
    public void onItemUpdate(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY)
            diskTree.updateNode(file);
        else if (tier == StoreOptionType.MEMORY_ONLY)
            memTree.updateNode(file);
    }

    @Override
    public void onItemDelete(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY)
            diskTree.deleteNode(file);
        else if (tier == StoreOptionType.MEMORY_ONLY)
            memTree.deleteNode(file);
    }

    @Override
    public void reset() {
        diskTree.clear();
        memTree.clear();
    }

}