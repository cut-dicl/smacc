package edu.cut.smacc.server.statistics.type.general;

import edu.cut.smacc.server.statistics.type.LongStatistics;
import edu.cut.smacc.server.statistics.type.StatisticType;

/**
 * Statistics data structure for tiers (memory, disk).
 */
public class TierGeneralStatistics extends LongStatistics {

    public TierGeneralStatistics() {
        super(StatisticType.listTierStats());
    }

}
