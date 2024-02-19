package edu.cut.smacc.server.statistics.output;

import edu.cut.smacc.configuration.Configuration;

/**
 * Statistics output policy that outputs only based on client requests.
 */
public class StatisticsOutputClientBased implements StatisticsOutputInvokePolicy {

    private boolean outputOnRequest = false;

    @Override
    public void initialize(Configuration configuration) {
        // Nothing to initialize
    }

    @Override
    public boolean shouldOutputStatistics() {
        boolean willOutputOnRequest = outputOnRequest;
        outputOnRequest = false;
        return willOutputOnRequest;
    }

    @Override
    public void outputStatisticsOnRequest() {
        outputOnRequest = true;
    }

}
