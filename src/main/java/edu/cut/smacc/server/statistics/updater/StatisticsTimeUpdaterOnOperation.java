package edu.cut.smacc.server.statistics.updater;

import edu.cut.smacc.server.statistics.type.StatisticType;
import edu.cut.smacc.server.statistics.type.Statistics;

public class StatisticsTimeUpdaterOnOperation implements StatisticsUpdaterOnOperation {

    protected final Statistics tierGeneralStatistics;

    public StatisticsTimeUpdaterOnOperation(Statistics tierGeneralStatistics) {
        this.tierGeneralStatistics = tierGeneralStatistics;
    }

    @Override
    public void updateOnPut(long fileSize, double putTime) {
        tierGeneralStatistics.incrementStatBy(StatisticType.PUT_TIME, putTime);
    }

    @Override
    public void updateOnGet(long fileSize, double getTime) {
        tierGeneralStatistics.incrementStatBy(StatisticType.GET_TIME, getTime);
    }

    @Override
    public void updateOnDelete(long fileSize, double deleteTime) {
        tierGeneralStatistics.incrementStatBy(StatisticType.DEL_TIME, deleteTime);
    }
}
