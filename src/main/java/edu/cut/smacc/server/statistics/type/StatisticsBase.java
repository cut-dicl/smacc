package edu.cut.smacc.server.statistics.type;

import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnOperation;

/**
 * Common operations between statistic holders. Any statistic has a type and a Number object associated with it.
 * Right now, Number objects are either AtomicLong or AtomicDouble.
 */
public abstract class StatisticsBase implements Statistics {

    protected StatisticsUpdaterOnOperation parentUpdater;

    protected StatisticsBase() {
        parentUpdater = null;
    }

    @Override
    public void incrementStat(StatisticType stat) {
        incrementStatBy(stat, 1);
    }

    @Override
    public void decrementStat(StatisticType stat) {
        decrementStatBy(stat, 1);
    }

    @Override
    public void setParentUpdater(StatisticsUpdaterOnOperation parentUpdater) {
        this.parentUpdater = parentUpdater;
    }

    @Override
    public StatisticsUpdaterOnOperation getParentUpdater() {
        return parentUpdater;
    }

}
