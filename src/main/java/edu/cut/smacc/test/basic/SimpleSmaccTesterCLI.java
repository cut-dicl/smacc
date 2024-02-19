package edu.cut.smacc.test.basic;

import edu.cut.smacc.cli.SmaccCLIConfigurationOnly;

public class SimpleSmaccTesterCLI extends SmaccCLIConfigurationOnly {
    public SimpleSmaccTesterCLI(String[] args) {
        super(args);
    }

    @Override
    public void parseArguments() {
        super.parseArguments();
        if (hasAllRequiredArguments) {
            SimpleSmaccTester.setClientConfigurationPath(args[1]);
        }
    }

    @Override
    public String description() {
        return "SimpleSmaccTester - A class used for testing SMACC connection.";
    }

    @Override
    public String usageLine() {
        return "Usage: java -cp <path_to_jar>/smacc-2.0-jar-with-dependencies.jar <SimpleSmaccTesterClass> [options]";
    }

    @Override
    public String example() {
        return "Example: java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.test.basic.SimpleSmaccTester -c conf/server.config.properties";
    }
}
