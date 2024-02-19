package edu.cut.smacc.server.statistics.output;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.utils.ReflectionUtils;

/**
 * Interface for the policy that will invoke outputting the statistics.
 */
public interface StatisticsOutputInvokePolicy {

    void initialize(Configuration configuration);

    boolean shouldOutputStatistics();

    void outputStatisticsOnRequest();

    static StatisticsOutputInvokePolicy getInstance(Configuration conf) {
        Class<? extends StatisticsOutputInvokePolicy> outputInvokePolicy = conf.getClass(
                ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_KEY,
                ServerConfigurations.STATISTICS_OUTPUT_INVOKE_POLICY_DEFAULT,
                StatisticsOutputInvokePolicy.class);

        StatisticsOutputInvokePolicy policy = ReflectionUtils.newInstance(outputInvokePolicy);
        policy.initialize(conf);
        return policy;
    }

}
