package edu.cut.smacc.server.statistics.type.general;

import edu.cut.smacc.server.statistics.type.LongStatistics;
import edu.cut.smacc.server.statistics.type.StatisticType;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Statistics data structure that aggregates the request statistics for all tiers & S3.
 */
public class TotalRequestsStatistics extends LongStatistics {

    public TotalRequestsStatistics(TierGeneralStatistics memoryStats, TierGeneralStatistics diskStats,
                                   S3GeneralStatistics s3Stats) {
        super(StatisticType.listAggregateStats());
        aggregateStats(memoryStats, diskStats, s3Stats);
    }

    private void aggregateStats(TierGeneralStatistics memoryStats, TierGeneralStatistics diskStats,
                                S3GeneralStatistics s3Stats) {
        for (StatisticType type : getStatTypes()) {
            AtomicLong value = new AtomicLong(0);
            if (memoryStats.getStat(type) != null) {
                value.addAndGet(memoryStats.getStat(type).longValue());
            }
            if (diskStats.getStat(type) != null) {
                value.addAndGet(diskStats.getStat(type).longValue());
            }
            if (s3Stats.getStat(type) != null) {
                value.addAndGet(s3Stats.getStat(type).longValue());
            }
            setStatWithValue(type, value);
        }
    }

}
