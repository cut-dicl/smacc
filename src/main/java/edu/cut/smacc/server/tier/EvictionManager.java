package edu.cut.smacc.server.tier;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.disk.DiskManager;
import edu.cut.smacc.server.cache.memory.MemoryManager;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import edu.cut.smacc.server.cache.policy.eviction.placement.EvictionPlacementPolicy;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.policy.eviction.trigger.EvictionTriggerPolicy;
import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnCacheOperation;
import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnOperation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

/**
 * This is the eviction handling service which is responsible to call the eviction policy and evict files as necessary
 *
 * @author Theodoros Danos
 * @author Michail Boronikolas
 */
public class EvictionManager implements Runnable {

    private static final Logger logger = LogManager.getLogger(EvictionManager.class);

    private final HashMap<Integer, StoreSettings> diskSettings;
    private final UsageStats downgradeStats;
    private final StoreSettings memorySettings;
    private final DiskManager dMgr;
    private final MemoryManager mMgr;
    private final TierManager tierMgr;
    private final ExecutorService downgrationHandler;
    private final EvictionPlacementPolicy evictionPlacementPolicy;
    private final EvictionTriggerPolicy evictionTriggerPolicy;
    private final EvictionItemPolicy evictionItemPolicy;
    private volatile boolean shutdown = false;

    EvictionManager(ExecutorService downgrationHandler, TierManager tierMgr, DiskManager dMgr, MemoryManager mmgr,
                    HashMap<Integer, StoreSettings> diskSettings, StoreSettings memorySettings) {
        this.dMgr = dMgr;
        this.mMgr = mmgr;
        this.tierMgr = tierMgr;
        this.diskSettings = diskSettings;
        this.memorySettings = memorySettings;

        this.downgrationHandler = downgrationHandler;
        this.downgradeStats = new UsageStats();
        this.evictionTriggerPolicy = tierMgr.getEvictionTriggerPolicy();
        CachePolicyNotifier policyNotifier = tierMgr.getCachePolicyNotifier();

        this.evictionPlacementPolicy = policyNotifier.getEvictionPlacementPolicy();
        this.evictionItemPolicy = policyNotifier.getEvictionItemPolicy();
    }

    public void shutdown() {
        shutdown = true;
    }

    public void run() {
        StatisticsUpdaterOnOperation memoryStatUpdater = tierMgr.getMemoryStatistics().getParentUpdater();
        StatisticsUpdaterOnOperation diskStatUpdater = tierMgr.getDiskStatistics().getParentUpdater();
        UsageStats stats;
        CacheFile evictionFile;
        boolean evicted;

        logger.info("Eviction Manager Started");

        if (dMgr.isActive() || mMgr.isActive())
            while (!shutdown) {
                //check disks
                if (dMgr.isActive()) {
                    for (Integer integer : diskSettings.keySet()) {
                        stats = diskSettings.get(integer).getStats();
                        while (evictionTriggerPolicy.triggerEviction(stats, null, StoreOptionType.DISK_ONLY)) {
                            evictionFile = evictionItemPolicy.getItemToEvict(StoreOptionType.DISK_ONLY);
                            if (evictionFile != null) {
                                logger.info("Evicting File[D]: " + evictionFile.getKey());
                                evicted = dMgr.evict(evictionFile); //prevent from adding new blocks to file (make file invisible to new partial writes)
                                if (evicted) {
                                    ((StatisticsUpdaterOnCacheOperation) diskStatUpdater).updateOnEvict(evictionFile.getTotalSize());
                                    evictionFile.delete();
                                } else {
                                    logger.warn("The item to be evicted from DISK was not found: " + evictionFile.getKey());
                                    // Manually remove it from the policy
                                    evictionItemPolicy.onItemDelete(evictionFile, dMgr.getStoreOptionType());
                                    evictionFile.delete();
                                }
                            } else {
                                logger.error("The eviction item policy returned a null file to evict from disk");
                                break;
                        }
                        }
                    }
                }
                //check memory
                if (mMgr.isActive()) {
                    stats = memorySettings.getStats();
                    while (evictionTriggerPolicy.triggerEviction(stats, downgradeStats, StoreOptionType.MEMORY_ONLY)) {
                        evictionFile = evictionItemPolicy.getItemToEvict(StoreOptionType.MEMORY_ONLY);
                        if (evictionFile != null) {
                            logger.info("Memory eviction at Reported Usage=" + stats.getReportedUsage() + " Actual Usage=" + stats.getActualUsage());
                            logger.info("Evicting File[M]: " + evictionFile.getKey() + "(Size: " + evictionFile.getTotalSize() + ")");
                            evicted = mMgr.evict(evictionFile); //prevent from adding new blocks to file (make file invisible to new partial writes)
                            if (evicted) {
                                ((StatisticsUpdaterOnCacheOperation) memoryStatUpdater).updateOnEvict(evictionFile.getTotalSize());
                                if (dMgr.isActive() && evictionPlacementPolicy.downgrade(evictionFile)) {
                                    //downgrades and deletes the evicted file
                                    downgrationHandler.submit(new DowngrationRunner(evictionFile, tierMgr, downgradeStats));
                                } else {
                                    evictionFile.delete();
                                }
                            } else {
                                logger.warn("The item to be evicted from MEM was not found: " + evictionFile.getKey());
                                // Manually remove it from the policy
                                evictionItemPolicy.onItemDelete(evictionFile, mMgr.getStoreOptionType());
                                evictionFile.delete();
                            }
                        } else {
                            logger.error("The eviction item policy returned a null file to evict from memory");
                            break;
                        }
                    }

                    try {
                        Thread.sleep(ServerConfigurations.getEvictionHandlerRun());
                    } catch (InterruptedException e) { /* Do nothing */ }
                }

                logger.info("Eviction Manager has Shutdown");
            }

    }
}