package edu.cut.smacc.cli;

import java.util.ArrayList;

/**
 * The SmaccCLIConfigurationOnly class represents a command-line interface (CLI) class that only requires a
 *  configuration path argument.
 */
public abstract class SmaccCLIConfigurationOnly extends SmaccCLIBase {

    private static final String OPTION_CONFIG = "  -c, --config     Specify the path to the configuration file";

    private static final int ARGUMENTS_LENGTH = 2;

    public SmaccCLIConfigurationOnly(String[] args) {
        super(args);
    }

    @Override
    public void parseArguments() {
        if (hasHelpArgument()) {
            printUsage();
        }
        else if (args.length != ARGUMENTS_LENGTH) {
            printInvalid();
            printUsage();
        }
        else if (hasConfigurationPathArgument()) {
            hasAllRequiredArguments = true;
        }
    }

    @Override
    public ArrayList<String> listOptions() {
        return new ArrayList<>() {{
            add(OPTION_HELP);
            add(OPTION_CONFIG);
        }};
    }

}
