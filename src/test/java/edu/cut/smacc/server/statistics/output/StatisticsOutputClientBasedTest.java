package edu.cut.smacc.server.statistics.output;

import org.junit.jupiter.api.Test;

public class StatisticsOutputClientBasedTest {

    @Test
    void testShouldOutputStatistics() {
        StatisticsOutputClientBased statisticsOutputClientBased = new StatisticsOutputClientBased();

        // This policy should never output statistics without getting requested to do so
        for (int i = 0; i < 100; i++) {
            boolean shouldOutputStatistics = statisticsOutputClientBased.shouldOutputStatistics();
            assert !shouldOutputStatistics;
        }
        // Client request arrives
        statisticsOutputClientBased.outputStatisticsOnRequest();
        // Now the policy should output statistics
        boolean shouldOutputStatistics = statisticsOutputClientBased.shouldOutputStatistics();
        assert shouldOutputStatistics;
        // The next time the policy should not output statistics
        shouldOutputStatistics = statisticsOutputClientBased.shouldOutputStatistics();
        assert !shouldOutputStatistics;

        System.out.println("StatisticsOutputClientBasedTest.testShouldOutputStatistics() passed.");
    }

}
