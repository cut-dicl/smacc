package edu.cut.smacc.server.cache.policy.admission;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.utils.collections.SortedWeightedTree;
import edu.cut.smacc.utils.collections.WeightedEXDNode;

import java.util.Iterator;

public class AdmissionEXD extends AdmissionLocation {

    private SortedWeightedTree<WeightedEXDNode<CacheFile>, CacheFile> memoryTree;
    private SortedWeightedTree<WeightedEXDNode<CacheFile>, CacheFile> diskTree;

    @Override
    public void initialize(Configuration conf) {
        super.initialize(conf);
        memoryTree = new SortedWeightedTree<>();
        diskTree = new SortedWeightedTree<>();

        WeightedEXDNode.ALPHA = 1d / (60d * 60 * 1000 * conf.getInt(
                ServerConfigurations.CACHING_POLICY_WEIGHTED_BIAS_HOURS_KEY,
                ServerConfigurations.CACHING_POLICY_WEIGHTED_BIAS_HOURS_DEFAULT));
    }

    @Override
    public StoreOptionType getReadAdmissionLocation(CacheFile file) {
        return getLocation(readLocation, file);
    }

    @Override
    public StoreOptionType getWriteAdmissionLocation(CacheFile file) {
        return getLocation(writeLocation, file);
    }

    @Override
    public void onItemAdd(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.MEMORY_ONLY) {
            memoryTree.addNode(new WeightedEXDNode<>(file, System.currentTimeMillis()));
        } else if (tier == StoreOptionType.DISK_ONLY) {
            diskTree.addNode(new WeightedEXDNode<>(file, System.currentTimeMillis()));
        }
    }

    @Override
    public void onItemNotAdded(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemAccess(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.MEMORY_ONLY) {
            memoryTree.updateNode(file);
        } else if (tier == StoreOptionType.DISK_ONLY) {
            diskTree.updateNode(file);
        }
    }

    @Override
    public void onItemUpdate(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.MEMORY_ONLY) {
            memoryTree.updateNode(file);
        } else if (tier == StoreOptionType.DISK_ONLY) {
            diskTree.updateNode(file);
        }
    }

    @Override
    public void onItemDelete(CacheFile file, StoreOptionType tier) {
        if (tier == StoreOptionType.MEMORY_ONLY) {
            memoryTree.deleteNode(file);
        } else if (tier == StoreOptionType.DISK_ONLY) {
            diskTree.deleteNode(file);
        }
    }

    @Override
    public void reset() {
        memoryTree.clear();
        diskTree.clear();
    }

    private StoreOptionType getLocation(StoreOptionType location, CacheFile file) {
        long freeSpace;
        switch (location) {
            case MEMORY_ONLY -> {
                freeSpace = memorySettings.getStats().getMaxCapacity() -
                        memorySettings.getStats().getReportedUsage();
                if (admit(file, memoryTree, freeSpace)) {
                    return StoreOptionType.MEMORY_ONLY;
                }
            }
            case DISK_ONLY -> {
                freeSpace = calculateMaxDiskCapacity();
                if (admit(file, diskTree, freeSpace)) {
                    return StoreOptionType.DISK_ONLY;
                }
            }
            case MEMORY_DISK -> {
                long memoryFreeSpace = memorySettings.getStats().getMaxCapacity() -
                        memorySettings.getStats().getReportedUsage();
                long diskFreeSpace = calculateMaxDiskCapacity();
                boolean admitMemory = admit(file, memoryTree, memoryFreeSpace);
                boolean admitDisk = admit(file, diskTree, diskFreeSpace);

                // Admit to both
                if (admitMemory && admitDisk) {
                    return StoreOptionType.MEMORY_DISK;
                }
                // Admit to memory
                if (admitMemory) {
                    return StoreOptionType.MEMORY_ONLY;
                }
                // Admit to disk
                if (admitDisk) {
                    return StoreOptionType.DISK_ONLY;
                }
            }
            case S3_ONLY -> {
                return StoreOptionType.S3_ONLY;
            }
            default -> throw new IllegalArgumentException("Unexpected value: " + location);
        }
        return StoreOptionType.S3_ONLY;
    }

    // Make admission decision based on the file, the free space and the weights of the
    // other files in the tree
    private boolean admit(CacheFile file, SortedWeightedTree<WeightedEXDNode<CacheFile>,
            CacheFile> tree, long freeSpace) {
        long fileSize = file.getActualSize();

        // If there is enough space, it is ok to admit
        if (freeSpace >= fileSize) {
            return true;
        }

        // Compute the weight of the items that will need to be evicted
        // if we decide to admit this item
        double sumWeights = 0d;
        WeightedEXDNode<CacheFile> currNode = new WeightedEXDNode<>(file, System.currentTimeMillis());
        Iterator<WeightedEXDNode<CacheFile>> nodes = tree.ascIter();
        WeightedEXDNode<CacheFile> nextNode;
        double nextNodeWeight;
        while (nodes.hasNext()) {
            nextNode = nodes.next();
            nextNodeWeight = nextNode.calcUpdatedWeight();
            if (sumWeights + nextNodeWeight < currNode.getWeight()) {
                sumWeights += nextNodeWeight;
                freeSpace += (nextNode.getItem()).getActualSize();
                if (freeSpace >= fileSize)
                    break; // we found enough files that could be replaced
            } else {
                break; // we will not find any more files
            }
        }
        return (freeSpace >= fileSize);
    }


    private long calculateMaxDiskCapacity() {
        long maxFreeSpace = -1;
        for (Integer diskNum : diskSettings.keySet()) {
            long diskCapacity = diskSettings.get(diskNum).getStats().getMaxCapacity();
            long diskUsage = diskSettings.get(diskNum).getStats().getReportedUsage();
            if (diskCapacity - diskUsage > maxFreeSpace) {
                maxFreeSpace = diskCapacity - diskUsage;
            }
        }
        return maxFreeSpace;
    }
}
