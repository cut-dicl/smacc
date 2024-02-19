package edu.cut.smacc.server.statistics;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.statistics.output.StatisticsOutputInvokePolicy;
import edu.cut.smacc.server.statistics.type.*;
import edu.cut.smacc.server.statistics.type.general.S3GeneralStatistics;
import edu.cut.smacc.server.statistics.type.general.TierGeneralStatistics;
import edu.cut.smacc.server.statistics.type.general.TotalRequestsStatistics;
import edu.cut.smacc.server.statistics.type.performance.MaxPerformanceStatistics;
import edu.cut.smacc.server.statistics.type.performance.PerformanceStatistics;
import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnCacheOperation;
import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnOperation;
import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnS3Operation;
import edu.cut.smacc.utils.BasicGlobalTimer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Manages the statistics of the server cache. It collects the statistics and outputs them to a file.
 */
public class StatisticsManager {
    private static final Logger logger = LogManager.getLogger(StatisticsManager.class);

    private static final String STATISTICS_FILENAME = "statistics.tsv";

    private final StatisticsOutputInvokePolicy statisticsOutputInvokePolicy;
    // Statistics to be maintained and collected
    private final Statistics memoryStatistics;
    private final Statistics diskStatistics;
    private final Statistics s3Statistics;
    private final Statistics storageStatistics;

    // Statistics ready for output
    private final List<Statistics> finalStatList;

    private final StatisticsWriter statisticsWriter;

    public StatisticsManager(Configuration configuration, TierGeneralStatistics memoryStatistics,
                             TierGeneralStatistics diskStatistics, PerformanceStatistics storageStatistics) {
        this.statisticsOutputInvokePolicy = StatisticsOutputInvokePolicy.getInstance(configuration);
        this.statisticsWriter = new StatisticsWriter(STATISTICS_FILENAME);
        this.memoryStatistics = Objects.requireNonNullElseGet(memoryStatistics, TierGeneralStatistics::new);
        this.diskStatistics = Objects.requireNonNullElseGet(diskStatistics, TierGeneralStatistics::new);
        this.storageStatistics = Objects.requireNonNullElseGet(storageStatistics, PerformanceStatistics::new);

        this.s3Statistics = new S3GeneralStatistics();
        StatisticsUpdaterOnOperation s3Updater = new StatisticsUpdaterOnS3Operation(s3Statistics);
        this.s3Statistics.setParentUpdater(s3Updater);

        this.finalStatList = new ArrayList<>(6);
    }

    public StatisticsManager(Configuration configuration, TierGeneralStatistics memoryStatistics,
                             TierGeneralStatistics diskStatistics, PerformanceStatistics storageStatistics,
                             String statisticsFilename) {
        this.statisticsOutputInvokePolicy = StatisticsOutputInvokePolicy.getInstance(configuration);
        this.statisticsWriter = new StatisticsWriter(statisticsFilename);
        this.memoryStatistics = Objects.requireNonNullElseGet(memoryStatistics, TierGeneralStatistics::new);
        this.diskStatistics = Objects.requireNonNullElseGet(diskStatistics, TierGeneralStatistics::new);
        this.storageStatistics = Objects.requireNonNullElseGet(storageStatistics, PerformanceStatistics::new);

        this.s3Statistics = new S3GeneralStatistics();
        StatisticsUpdaterOnOperation s3Updater = new StatisticsUpdaterOnS3Operation(s3Statistics);
        this.s3Statistics.setParentUpdater(s3Updater);

        this.finalStatList = new ArrayList<>(6);
    }

    public S3GeneralStatistics getS3Statistics() {
        return (S3GeneralStatistics) s3Statistics;
    }

    public PerformanceStatistics getPerformanceStatistics() {
        return (PerformanceStatistics) storageStatistics;
    }

    public void outputStatisticsOnClientRequest() {
        statisticsOutputInvokePolicy.outputStatisticsOnRequest();
        outputStatistics();
    }

    public boolean outputStatistics() {
        if (statisticsOutputInvokePolicy.shouldOutputStatistics()) {
            collectStatisticsBeforeOutput();
            statisticsWriter.writeStatistics(finalStatList);
            resetStatistics();
            return true;
        }
        return false;
    }

    private void collectStatisticsBeforeOutput() {
        List<Statistics> tierGeneralStatisticsList = new ArrayList<>();
        if (memoryStatistics.getParentUpdater() != null) {
            tierGeneralStatisticsList.add(memoryStatistics);
        } else {
            tierGeneralStatisticsList.add(new TierGeneralStatistics());
        }
        if (diskStatistics.getParentUpdater() != null) {
            tierGeneralStatisticsList.add(diskStatistics);
        } else {
            tierGeneralStatisticsList.add(new TierGeneralStatistics());
        }
        // Collect cache files and bytes
        collectCacheFilesAndBytes(tierGeneralStatisticsList);
        // Now add the s3 statistics to the list
        tierGeneralStatisticsList.add(s3Statistics);
        // Aggregate operations
        StatisticsAggregator aggregator = new StatisticsAggregator(tierGeneralStatisticsList, storageStatistics);
        aggregator.aggregate();
        finalStatList.addAll(aggregator.getFinalStatsList());
    }

    private void collectCacheFilesAndBytes(List<Statistics> tierGeneralStatisticsList) {
        for (Statistics tierGeneralStatistics : tierGeneralStatisticsList) {
            StatisticsUpdaterOnOperation updater = tierGeneralStatistics.getParentUpdater();
            if (updater instanceof StatisticsUpdaterOnCacheOperation) {
                ((StatisticsUpdaterOnCacheOperation) updater).collectCacheFilesAndBytes();
            }
        }
    }

    public void resetStatistics() {
        // Reset individual statistics
        memoryStatistics.resetStats();
        diskStatistics.resetStats();
        s3Statistics.resetStats();
        storageStatistics.resetStats();
        // Reset final statistics
        for (Statistics stats : finalStatList) {
            stats.resetStats();
        }
        finalStatList.clear();
        BasicGlobalTimer.resetTimer();
        logger.info("Statistics have been reset");
    }

    private static class StatisticsAggregator {

        private final List<Statistics> tierStatsList;
        private final Statistics storageStatistics;

        private final List<Statistics> finalStatsList;

        StatisticsAggregator(List<Statistics> tierGeneralStatistics, Statistics storageStatistics) {
            this.tierStatsList = tierGeneralStatistics;
            this.storageStatistics = storageStatistics;
            finalStatsList = new ArrayList<>(6);
        }

        private void aggregate() {
            TotalRequestsStatistics totalRequestsStatistics = new TotalRequestsStatistics(
                    (TierGeneralStatistics) tierStatsList.get(0),
                    (TierGeneralStatistics) tierStatsList.get(1),
                    (S3GeneralStatistics) tierStatsList.get(2));
            aggregateThroughputForTier(totalRequestsStatistics);
            aggregateIOPSForTier(totalRequestsStatistics);
            aggregateLatencyForTier(totalRequestsStatistics);

            MaxPerformanceStatistics maxPerformanceStatistics = new MaxPerformanceStatistics(
                    totalRequestsStatistics, storageStatistics);
            finalStatsList.addAll(tierStatsList);
            finalStatsList.add(storageStatistics);
            finalStatsList.add(totalRequestsStatistics);
            finalStatsList.add(maxPerformanceStatistics);
        }

        public List<Statistics> getFinalStatsList() {
            return finalStatsList;
        }

        // THROUGHPUT
        private void aggregateThroughputForTier(Statistics totalStats) {
            aggregateOperationThroughputForTier(totalStats, StatisticType.PUT_BYTES,
                    StatisticType.PUT_THROUGHPUT);
            aggregateOperationThroughputForTier(totalStats, StatisticType.GET_BYTES,
                    StatisticType.GET_THROUGHPUT);
            aggregateOperationThroughputForTier(totalStats, StatisticType.DEL_BYTES,
                    StatisticType.DEL_THROUGHPUT);
        }

        private void aggregateOperationThroughputForTier(Statistics totalStats, StatisticType countStat,
                                                         StatisticType timeStat) {
            long operationBytes = totalStats.getStat(countStat).longValue();
            double throughput = calculateThroughput(operationBytes);
            storageStatistics.setStatWithValue(timeStat, throughput);
        }

        private double calculateThroughput(long bytes) {
            return bytes / ((double) BasicGlobalTimer.getElapsedTime() / 1000);
        }

        // IOPS
        private void aggregateIOPSForTier(Statistics statistics) {
            aggregateOperationIOPSForTier(statistics, StatisticType.PUT_COUNT,
                    StatisticType.PUT_IOPS);
            aggregateOperationIOPSForTier(statistics, StatisticType.GET_COUNT,
                    StatisticType.GET_IOPS);
            aggregateOperationIOPSForTier(statistics, StatisticType.DEL_COUNT,
                    StatisticType.DEL_IOPS);
        }

        private void aggregateOperationIOPSForTier(Statistics tierStats, StatisticType countStat,
                                                   StatisticType timeStat) {
            // Aggregate IOPS
            long operationCount = tierStats.getStat(countStat).longValue();
            double iops = calculateIOPS(operationCount);
            storageStatistics.setStatWithValue(timeStat, iops);
        }

        private double calculateIOPS(long operationCount) {
            // Using the same method as throughput, but with operation count instead of bytes
            return calculateThroughput(operationCount);
        }

        // LATENCY
        private void aggregateLatencyForTier(Statistics statistics) {
            aggregateOperationLatencyForTier(statistics, StatisticType.PUT_TIME,
                    StatisticType.PUT_COUNT,
                    StatisticType.PUT_LATENCY);
            aggregateOperationLatencyForTier(statistics, StatisticType.GET_TIME,
                    StatisticType.GET_COUNT,
                    StatisticType.GET_LATENCY);
            aggregateOperationLatencyForTier(statistics, StatisticType.DEL_TIME,
                    StatisticType.DEL_COUNT,
                    StatisticType.DEL_LATENCY);
        }

        private void aggregateOperationLatencyForTier(Statistics tierStats, StatisticType timeStat,
                                                      StatisticType countStat,
                                                      StatisticType latencyStat) {
            // Aggregate latency
            double operationTime = storageStatistics.getStat(timeStat).longValue();
            long operationCount = tierStats.getStat(countStat).longValue();
            double latency = calculateLatency(operationTime, operationCount);
            storageStatistics.setStatWithValue(latencyStat, latency);
        }

        private double calculateLatency(double operationTime, long operationCount) {
            if (operationCount == 0) {
                return 0;
            }
            return operationTime / operationCount;
        }

    }
}
