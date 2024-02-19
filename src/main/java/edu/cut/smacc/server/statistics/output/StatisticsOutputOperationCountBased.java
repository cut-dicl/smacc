package edu.cut.smacc.server.statistics.output;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;

/**
 * Statistics output policy that invokes outputting the statistics every X operations.
 */

public class StatisticsOutputOperationCountBased extends StatisticsOutputClientBased {

    private long operationInterval;
    private long operationsUntilOutput;

    @Override
    public void initialize(Configuration configuration) {
        this.operationInterval = configuration.getInt(ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_KEY,
                ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_DEFAULT);
        this.operationsUntilOutput = 0;
    }

    @Override
    public boolean shouldOutputStatistics() {
        boolean shouldOutputByPolicy = operationsUntilOutput != 0 && operationsUntilOutput % operationInterval == 0;
        updateOperations();
        return super.shouldOutputStatistics() || shouldOutputByPolicy;
    }

    public void updateOperations() {
        operationsUntilOutput++;
    }

}
