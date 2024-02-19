package edu.cut.smacc.server.main.request;

import edu.cut.smacc.server.statistics.updater.StatisticsTimeUpdaterOnOperation;
import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnCacheOperation;
import edu.cut.smacc.server.statistics.updater.StatisticsUpdaterOnS3Operation;
import edu.cut.smacc.server.tier.TierManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class RequestHandlerBase implements RequestHandler {
    protected static final Logger logger = LogManager.getLogger(ClientConnectionHandler.class);

    protected ClientConnectionHandler connectionHandler;

    protected TierManager tier;
    protected StatisticsUpdaterOnCacheOperation memoryStatUpdater;
    protected StatisticsUpdaterOnCacheOperation diskStatUpdater;
    protected StatisticsUpdaterOnS3Operation s3StatUpdater;
    protected StatisticsTimeUpdaterOnOperation timeStatUpdater;

    /**
     * Initialization of needed objects for the request handlers. Most of these are the statistics updaters.
     * @param connectionHandler The connection handler that created this request handler.
     */
    protected RequestHandlerBase(ClientConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;

        this.tier = connectionHandler.getTier();
        this.memoryStatUpdater = (StatisticsUpdaterOnCacheOperation)
                tier.getMemoryStatistics().getParentUpdater();
        this.diskStatUpdater = (StatisticsUpdaterOnCacheOperation)
                tier.getDiskStatistics().getParentUpdater();

        this.s3StatUpdater = (StatisticsUpdaterOnS3Operation) connectionHandler
                .getStatisticsManager()
                .getS3Statistics()
                .getParentUpdater();
        this.timeStatUpdater = (StatisticsTimeUpdaterOnOperation) connectionHandler
                .getStatisticsManager()
                .getPerformanceStatistics()
                .getParentUpdater();
    }

}
