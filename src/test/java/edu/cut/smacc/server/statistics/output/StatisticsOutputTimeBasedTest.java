package edu.cut.smacc.server.statistics.output;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import org.junit.jupiter.api.Test;

public class StatisticsOutputTimeBasedTest {

    @Test
    public void testShouldOutputStatistics() throws InterruptedException {
        int seconds = 1;
        Configuration configuration = new Configuration();
        configuration.addProperty(ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_KEY,
                StatisticsOutputTimeBased.class.getName());
        configuration.addProperty(ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_KEY, seconds);
        StatisticsOutputInvokePolicy statisticsOutputTimeBased = StatisticsOutputInvokePolicy.getInstance(configuration);
        // Let's say we have 10 operations coming every 0.2 of a second
        // Operations at 1.0 and 2.0 seconds should output statistics
        // but let's add a client request on the 7th operation (1.4 seconds)
        // 0 -> 0.0
        // 1 -> 0.2
        // 2 -> 0.4
        // 3 -> 0.6
        // 4 -> 0.8
        // 5 -> 1.0 -> output
        // 6 -> 1.2
        // 7 -> 1.4 -> output
        // 8 -> 1.6
        // 9 -> 1.8
        // 10 -> 2.0 -> output
        for (int i = 0; i < 11; i++) {
            boolean mustBeTrue;
            if (i == 7) {
                statisticsOutputTimeBased.outputStatisticsOnRequest();
                mustBeTrue = statisticsOutputTimeBased.shouldOutputStatistics();
            }
            else if (i == 5 || i == 10) {
                mustBeTrue = statisticsOutputTimeBased.shouldOutputStatistics();
            } else {
                mustBeTrue = !statisticsOutputTimeBased.shouldOutputStatistics();
            }
            assert mustBeTrue;
            Thread.sleep(200);
        }

        System.out.println("StatisticsOutputTimeBasedTest.testShouldOutputStatistics() passed.");
    }

}
