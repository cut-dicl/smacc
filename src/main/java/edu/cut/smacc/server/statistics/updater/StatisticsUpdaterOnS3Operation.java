package edu.cut.smacc.server.statistics.updater;

import edu.cut.smacc.server.statistics.type.StatisticType;
import edu.cut.smacc.server.statistics.type.Statistics;

public class StatisticsUpdaterOnS3Operation extends StatisticsTimeUpdaterOnOperation {

    public StatisticsUpdaterOnS3Operation(Statistics tierGeneralStatistics) {
        super(tierGeneralStatistics);
    }

    @Override
    public void updateOnPut(long fileSize, double putTime) {
        tierGeneralStatistics.incrementStat(StatisticType.PUT_COUNT);
        tierGeneralStatistics.incrementStatBy(StatisticType.PUT_BYTES, fileSize);
    }

    @Override
    public void updateOnGet(long fileSize, double getTime) {
        tierGeneralStatistics.incrementStat(StatisticType.GET_COUNT);
        tierGeneralStatistics.incrementStatBy(StatisticType.GET_BYTES, fileSize);
    }

    @Override
    public void updateOnDelete(long fileSize, double deleteTime) {
        tierGeneralStatistics.incrementStat(StatisticType.DEL_COUNT);
        tierGeneralStatistics.incrementStatBy(StatisticType.DEL_BYTES, fileSize);
    }
}
