package edu.cut.smacc.server.main;

import edu.cut.smacc.cli.SmaccCLIConfigurationOnly;


/**
 * ServerMainCLI is a command-line interface (CLI) class for starting the SMACC server.
 * It provides options to configure and run the server using command-line arguments.
 */
public class ServerMainCLI extends SmaccCLIConfigurationOnly {

    public ServerMainCLI(String[] args) {
        super(args);
    }

    @Override
    public void parseArguments() {
        super.parseArguments();
        if (hasAllRequiredArguments) {
            ServerMain.setServerConfigurationPath(args[1]);
        }
    }

    @Override
    public String description() {
        return "ServerMain - The main server application for starting the SMACC server.";
    }

    @Override
    public String usageLine() {
        return "Usage: java -cp <path_to_jar>/smacc-2.0-jar-with-dependencies.jar <ServerMainClass> [options]";
    }

    @Override
    public String example() {
        return "Example: java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.server.main.ServerMain -c conf/server.config.properties";
    }

}
