package edu.cut.smacc.server.statistics.type.general;


import edu.cut.smacc.server.statistics.type.LongStatistics;
import edu.cut.smacc.server.statistics.type.StatisticType;

/**
 * Statistics data structure for S3.
 */
public class S3GeneralStatistics extends LongStatistics {

    public S3GeneralStatistics() {
        super(StatisticType.listS3Stats());
    }

}
