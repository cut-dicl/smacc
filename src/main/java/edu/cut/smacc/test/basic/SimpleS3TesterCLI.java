package edu.cut.smacc.test.basic;

import edu.cut.smacc.cli.SmaccCLIConfigurationOnly;

public class SimpleS3TesterCLI extends SmaccCLIConfigurationOnly {
    public SimpleS3TesterCLI(String[] args) {
        super(args);
    }

    @Override
    public void parseArguments() {
        super.parseArguments();
        if (hasAllRequiredArguments) {
            SimpleS3Tester.setClientConfigurationPath(args[1]);
        }
    }

    @Override
    public String description() {
        return "SimpleS3Tester - A class used for testing S3 connection.";
    }

    @Override
    public String usageLine() {
        return "Usage: java -cp <path_to_jar>/smacc-2.0-jar-with-dependencies.jar <SimpleS3TesterClass> [options]";
    }

    @Override
    public String example() {
        return "Example: java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.test.basic.SimpleS3Tester -c conf/server.config.properties";
    }
}
