package edu.cut.smacc.server.statistics.output;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import org.junit.jupiter.api.Test;

public class StatisticsOutputOperationCountBasedTest {

    @Test
    public void testShouldOutputStatistics() {
        int operations = 1;
        Configuration configuration = new Configuration();
        configuration.addProperty(ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_KEY,
                StatisticsOutputOperationCountBased.class.getName());
        configuration.addProperty(ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_KEY, operations);
        StatisticsOutputInvokePolicy statisticsOutputOperationCountBased = StatisticsOutputInvokePolicy.getInstance(configuration);
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();

        operations = 2;
        configuration = new Configuration();
        configuration.addProperty(ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_KEY, operations);
        statisticsOutputOperationCountBased = StatisticsOutputInvokePolicy.getInstance(configuration);
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();
        // Client request for statistics
        statisticsOutputOperationCountBased.outputStatisticsOnRequest();
        // While operations interval is not reached, statistics should be output because of the client request
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();
        // Now operations are reached
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();
        // Now they are not
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();

        operations = 3;
        configuration = new Configuration();
        configuration.addProperty(ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_KEY, operations);
        statisticsOutputOperationCountBased = StatisticsOutputInvokePolicy.getInstance(configuration);
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();
        // The operations are not reached, but the client request for statistics
        statisticsOutputOperationCountBased.outputStatisticsOnRequest();
        // While operations interval is not reached, statistics should be output because of the client request
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();
        // Still, operations are not reached
        assert !statisticsOutputOperationCountBased.shouldOutputStatistics();
        // Now operations are reached
        assert statisticsOutputOperationCountBased.shouldOutputStatistics();

        System.out.println("StatisticsOutputOperationCountBasedTest.testShouldOutputStatistics() passed.");
    }

}
