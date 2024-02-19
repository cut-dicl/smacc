package edu.cut.smacc.test.communication;

import edu.cut.smacc.cli.SmaccCLIConfigurationOnly;

/**
 * Basic CLI for Server test
 */
public class ServerTestCLI extends SmaccCLIConfigurationOnly {

    public ServerTestCLI(String[] args) {
        super(args);
    }

    @Override
    public void parseArguments() {
        super.parseArguments();
        if (hasAllRequiredArguments) {
            Server.SERVER_TEST_CONFIGURATION_PATH = args[1];
        }
    }

    @Override
    public String description() {
        return "Server - A server communication test class.";
    }

    @Override
    public String usageLine() {
        return "Usage: java -cp <path_to_jar>/smacc-2.0-jar-with-dependencies.jar <ServerClass> [options]";
    }

    @Override
    public String example() {
        return "Example: java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.test.communication.Server -c conf/server.config.properties";
    }

}
