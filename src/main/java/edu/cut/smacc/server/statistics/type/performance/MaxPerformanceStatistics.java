package edu.cut.smacc.server.statistics.type.performance;


import edu.cut.smacc.server.statistics.type.DoubleStatistics;
import edu.cut.smacc.server.statistics.type.StatisticType;
import edu.cut.smacc.server.statistics.type.Statistics;

/**
 * Statistics data structure that holds max performance statistics.
 */
public class MaxPerformanceStatistics extends DoubleStatistics {

    public MaxPerformanceStatistics(Statistics totalRequestStats, Statistics storageStats) {
        super(StatisticType.listAllMaxPerformanceStats());
        calculateMaxPerformanceStats(totalRequestStats, storageStats);
    }

    private void calculateMaxPerformanceStats(Statistics totalRequestStats, Statistics storageStats) {
        // GET
        double getThroughput = calculateMaxThroughput(totalRequestStats.getStat(StatisticType.GET_BYTES),
                storageStats.getStat(StatisticType.GET_TIME));
        double getIOPS = calculateMaxIOPS(totalRequestStats.getStat(StatisticType.GET_COUNT),
                storageStats.getStat(StatisticType.GET_TIME));
        // PUT
        double putThroughput = calculateMaxThroughput(totalRequestStats.getStat(StatisticType.PUT_BYTES),
                storageStats.getStat(StatisticType.PUT_TIME));
        double putIOPS = calculateMaxIOPS(totalRequestStats.getStat(StatisticType.PUT_COUNT),
                storageStats.getStat(StatisticType.PUT_TIME));

        // DELETE
        double deleteThroughput = calculateMaxThroughput(totalRequestStats.getStat(StatisticType.DEL_BYTES),
                storageStats.getStat(StatisticType.DEL_TIME));
        double deleteIOPS = calculateMaxIOPS(totalRequestStats.getStat(StatisticType.DEL_COUNT),
                storageStats.getStat(StatisticType.DEL_TIME));

        // Put them into the map
        setStatWithValue(StatisticType.GET_MAX_THROUGHPUT, getThroughput);
        setStatWithValue(StatisticType.GET_MAX_IOPS, getIOPS);
        setStatWithValue(StatisticType.PUT_MAX_THROUGHPUT, putThroughput);
        setStatWithValue(StatisticType.PUT_MAX_IOPS, putIOPS);
        setStatWithValue(StatisticType.DEL_MAX_THROUGHPUT, deleteThroughput);
        setStatWithValue(StatisticType.DEL_MAX_IOPS, deleteIOPS);
    }

    private double calculateMaxThroughput(Number bytes, Number time) {
        long longBytes = bytes.longValue();
        double doubleTime = time.doubleValue();
        if (longBytes == 0 || doubleTime == 0) {
            return 0;
        }
        return (longBytes * 1000) / doubleTime;
    }

    private double calculateMaxIOPS(Number count, Number time) {
        long longCount = count.longValue();
        double doubleTime = time.doubleValue();
        if (longCount == 0 || doubleTime == 0) {
            return 0;
        }
        return (longCount * 1000) / doubleTime;
    }

}
