package edu.cut.smacc.server.statistics.type;

import java.util.Set;

import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnOperation;

/**
 * Statistics interface that is used to store any kind of statistics.
 */
public interface Statistics {

    void setStatWithValue(StatisticType type, Number value);

    Number getStat(StatisticType type);

    boolean containsStat(StatisticType type);

    Set<StatisticType> getStatTypes();

    int getStatCount();

    void clearStats();

    void resetStats();

    void incrementStat(StatisticType type);

    void incrementStatBy(StatisticType type, Number value);

    void decrementStat(StatisticType type);

    void decrementStatBy(StatisticType type, Number value);

    void setParentUpdater(StatisticsUpdaterOnOperation updater);

    StatisticsUpdaterOnOperation getParentUpdater();

}
