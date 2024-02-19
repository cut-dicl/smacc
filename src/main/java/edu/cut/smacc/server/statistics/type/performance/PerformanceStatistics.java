package edu.cut.smacc.server.statistics.type.performance;

import edu.cut.smacc.server.statistics.type.DoubleStatistics;
import edu.cut.smacc.server.statistics.type.StatisticType;

/**
 * Statistics data structure that hold performance statistics.
 */
public class PerformanceStatistics extends DoubleStatistics {

    public PerformanceStatistics() {
        super(StatisticType.listAllPerformanceStats());
    }

}
