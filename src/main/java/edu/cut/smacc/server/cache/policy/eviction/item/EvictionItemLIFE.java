package edu.cut.smacc.server.cache.policy.eviction.item;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.utils.collections.SortedWeightedTree;
import edu.cut.smacc.utils.collections.WeightedLIFENode;
import edu.cut.smacc.utils.collections.WeightedSizeNode;

import java.util.Iterator;

public class EvictionItemLIFE implements EvictionItemPolicy {

    private long windowBasedAging;

    private SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> diskOldListLFU;
    private SortedWeightedTree<WeightedSizeNode<CacheFile>, CacheFile> diskNewListSize;
    private SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> diskNewListLRU;

    private SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> memOldListLFU;
    private SortedWeightedTree<WeightedSizeNode<CacheFile>, CacheFile> memNewListSize;
    private SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> memNewListLRU;

    @Override
    public void initialize(Configuration conf) {
        windowBasedAging = 60L * 60L * 1000000L * conf.getInt(
                ServerConfigurations.EVICTION_POLICY_WINDOW_BASED_AGING_HOURS_KEY,
                ServerConfigurations.EVICTION_POLICY_WINDOW_BASED_AGING_HOURS_DEFAULT);

        diskOldListLFU = new SortedWeightedTree<>(new WeightedLIFENode.LFUComparator<CacheFile>());
        diskNewListSize = new SortedWeightedTree<>();
        diskNewListLRU = new SortedWeightedTree<>(new WeightedLIFENode.LRUComparator<CacheFile>());

        memOldListLFU = new SortedWeightedTree<>(new WeightedLIFENode.LFUComparator<CacheFile>());
        memNewListSize = new SortedWeightedTree<>();
        memNewListLRU = new SortedWeightedTree<>(new WeightedLIFENode.LRUComparator<CacheFile>());
    }

    @Override
    public CacheFile getItemToEvict(StoreOptionType evictionTier) {
        if (evictionTier == StoreOptionType.DISK_ONLY) {
            return getTierItemToEvict(diskOldListLFU, diskNewListSize, diskNewListLRU);
        } else if (evictionTier == StoreOptionType.MEMORY_ONLY) {
            return getTierItemToEvict(memOldListLFU, memNewListSize, memNewListLRU);
        }

        return null;
    }

    @Override
    public void onItemAdd(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY) {
            diskNewListSize.addNode(new WeightedSizeNode<>(file, file.getActualSize()));
            diskNewListLRU.addNode(new WeightedLIFENode<>(file, file.getLastModified()));
        } else if (tier == StoreOptionType.MEMORY_ONLY) {
            memNewListSize.addNode(new WeightedSizeNode<>(file, file.getActualSize()));
            memNewListLRU.addNode(new WeightedLIFENode<>(file, file.getLastModified()));
        }
    }

    @Override
    public void onItemNotAdded(CacheFile file, StoreOptionType tier) {
        // Nothing to do
    }

    @Override
    public void onItemAccess(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY) {
            accessCacheItem(file, diskOldListLFU, diskNewListSize, diskNewListLRU);
        } else if (tier == StoreOptionType.MEMORY_ONLY) {
            accessCacheItem(file, memOldListLFU, memNewListSize, memNewListLRU);
        }
    }

    @Override
    public void onItemUpdate(CacheFile file, StoreOptionType tier) {
        onItemAccess(file, tier);
    }

    @Override
    public void onItemDelete(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.DISK_ONLY) {
            deleteItem(file, diskOldListLFU, diskNewListSize, diskNewListLRU);
        } else if (tier == StoreOptionType.MEMORY_ONLY) {
            deleteItem(file, memOldListLFU, memNewListSize, memNewListLRU);
        }
    }

    @Override
    public void reset() {
        diskOldListLFU.clear();
        diskNewListSize.clear();
        diskNewListLRU.clear();

        memOldListLFU.clear();
        memNewListSize.clear();
        memNewListLRU.clear();
    }

    private void accessCacheItem(CacheFile item, SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> oldListLFU,
                                 SortedWeightedTree<WeightedSizeNode<CacheFile>, CacheFile> newListSize,
                                 SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> newListLRU) {
        // On item access: if the item is in oldListLFU,
        // it is removed from there and added in the two new trees.
        if (oldListLFU.contains(item)) {
            WeightedLIFENode<CacheFile> delNode = oldListLFU.deleteNode(item);
            delNode.updateWeight();
            newListLRU.addNode(delNode);
            newListSize.addNode(new WeightedSizeNode<>(item, item.getSize()));
        } else if (newListLRU.contains(item)) {
            // Update the item in new LRU trees and the size (if the size changed)
            newListLRU.updateNode(item);
            WeightedSizeNode<CacheFile> node = newListSize.getNode(item);
            if (node.getWeight() != item.getSize()) {
                newListSize.deleteNode(item);
                node.setSize(item.getSize());
                newListSize.addNode(node);
            }
        }
    }

    private CacheFile getTierItemToEvict(SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> oldListLFU,
                                         SortedWeightedTree<WeightedSizeNode<CacheFile>, CacheFile> newListSize,
                                         SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> newListLRU) {
        // traverse the newListLRU in ascending order and move
        // any items older than the window into the oldListLFU.
        // These items must be removed from newListSize as well.
        long now = System.currentTimeMillis();
        Iterator<WeightedLIFENode<CacheFile>> nodes = newListLRU.ascIter();
        while (nodes.hasNext()) {
            WeightedLIFENode<CacheFile> node = nodes.next();
            if ((now - node.getWeight()) > windowBasedAging) {
                oldListLFU.addNode(node);
                nodes.remove();
                newListSize.deleteNode(node.getItem());
            } else {
                // All items in newListLRU from now on are newer than the window
                break;
            }
        }

        // If oldListLFU is not empty, return LFU.
        // Otherwise, return the largest item from newListSize.
        if (oldListLFU.size() > 0) {
            return oldListLFU.getMinWeightItem();
        }

        // Otherwise, return the largest item from newListSize.
        if (newListSize.size() > 0) {
            return newListSize.getMaxWeightItem();
        }

        return null;
    }

    private void deleteItem(CacheFile item, SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> oldListLFU,
                            SortedWeightedTree<WeightedSizeNode<CacheFile>, CacheFile> newListSize,
                            SortedWeightedTree<WeightedLIFENode<CacheFile>, CacheFile> newListLRU) {
        if (oldListLFU.contains(item)) {
            oldListLFU.deleteNode(item);
        } else if (newListLRU.contains(item)) {
            newListSize.deleteNode(item);
            newListLRU.deleteNode(item);
        }
    }

}
