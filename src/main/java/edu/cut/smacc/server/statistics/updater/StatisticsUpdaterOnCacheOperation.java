package edu.cut.smacc.server.statistics.updater;


import edu.cut.smacc.server.cache.common.CacheManager;
import edu.cut.smacc.server.statistics.type.StatisticType;
import edu.cut.smacc.server.statistics.type.Statistics;

/**
 * Keeps track of the statistics of the cache.
 */
public class StatisticsUpdaterOnCacheOperation extends StatisticsUpdaterOnS3Operation {

    private final CacheManager cacheManager;

    public StatisticsUpdaterOnCacheOperation(Statistics statistics, CacheManager cacheManager) {
        super(statistics);
        this.cacheManager = cacheManager;
    }

    /**
     * Update the statistics when a file is put into the cache.
     * @param fileSize The size of the file that is put.
     */
    @Override
    public void updateOnPut(long fileSize, double putTime) {
        super.updateOnPut(fileSize, putTime);
        updateStatisticsWithCacheChanges();
    }

    /**
     * Update the statistics when a file is requested and found in the cache.
     * @param fileSize The size of the file that is requested.
     * @param getTime The time it took to get the file.
     */
    public void updateGetOnHit(long fileSize, double getTime) {
        super.updateOnGet(fileSize, getTime);
        tierGeneralStatistics.incrementStat(StatisticType.HIT_COUNT);
        tierGeneralStatistics.incrementStatBy(StatisticType.HIT_BYTES, fileSize);
    }

    /**
     * Update the statistics when a file is requested but not found in the cache.
     */
    public void updateGetOnMiss(long fileSize) {
        // Get time set to 0 because the file is not found in the cache.
        super.updateOnGet(fileSize, 0);
        tierGeneralStatistics.incrementStat(StatisticType.MISS_COUNT);
        tierGeneralStatistics.incrementStatBy(StatisticType.MISS_BYTES, fileSize);
    }

    /**
     * Update the statistics when a file is evicted from the cache.
     * @param fileSize The size of the file that is evicted.
     */
    public void updateOnEvict(long fileSize) {
        tierGeneralStatistics.incrementStat(StatisticType.EVICT_COUNT);
        tierGeneralStatistics.incrementStatBy(StatisticType.EVICT_BYTES, fileSize);
        updateStatisticsWithCacheChanges();
    }

    /**
     * Update the statistics when a file is deleted from the cache.
     */
    @Override
    public void updateOnDelete(long fileSize, double deleteTime) {
        super.updateOnDelete(fileSize, deleteTime);
        updateStatisticsWithCacheChanges();
    }

    public void collectCacheFilesAndBytes() {
        updateStatisticsWithCacheChanges();
    }

    private void updateStatisticsWithCacheChanges() {
        tierGeneralStatistics.setStatWithValue(StatisticType.CACHE_FILES, cacheManager.getCacheFilesCount());
        tierGeneralStatistics.setStatWithValue(StatisticType.CACHE_BYTES, cacheManager.getCacheBytes());
    }

}
