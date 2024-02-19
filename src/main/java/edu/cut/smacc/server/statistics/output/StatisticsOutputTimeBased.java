package edu.cut.smacc.server.statistics.output;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;

/**
 * Statistics output policy that invokes outputting the statistics every X seconds.
 */
public class StatisticsOutputTimeBased extends StatisticsOutputClientBased {

    private long outputIntervalInSeconds;
    private long lastOutputTimestamp;
    private boolean justOutputted;

    public StatisticsOutputTimeBased() {
        this.lastOutputTimestamp = System.currentTimeMillis();
        this.justOutputted = false;
    }

    @Override
    public void initialize(Configuration configuration) {
        this.outputIntervalInSeconds = configuration.getInt(ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_KEY,
                ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_DEFAULT);
        this.lastOutputTimestamp = System.currentTimeMillis();
        this.justOutputted = false;
    }

    @Override
    public boolean shouldOutputStatistics() {
        long currentTime = System.currentTimeMillis();
        boolean shouldOutputByPolicy = (currentTime - lastOutputTimestamp) / 1000 >= outputIntervalInSeconds;
        justOutputted = shouldOutputByPolicy;
        updateLastOutputTimestamp();
        return super.shouldOutputStatistics() || shouldOutputByPolicy;
    }

    private void updateLastOutputTimestamp() {
        if (justOutputted) {
            lastOutputTimestamp = System.currentTimeMillis(); // Update last output timestamp
        }
    }

}
