package edu.cut.smacc.cli;

/**
 * Implementation of SmaccCLI common operations.
 */
public abstract class SmaccCLIBase implements SmaccCLI {

    protected final String[] args;
    protected boolean hasAllRequiredArguments = false;
    protected boolean hasNonExistingArguments = false;
    protected static final String OPTION_HELP = "  -h, --help       Print this usage information";

    public SmaccCLIBase(String[] args) {
        this.args = args;
        if (hasArguments()) {
            this.parseArguments();
        }
    }

    @Override
    public boolean hasArguments() {
        return args.length >= 1;
    }

    @Override
    public boolean hasHelpArgument() {
        return args[0].compareToIgnoreCase("-h") == 0 || args[0].compareToIgnoreCase("--help") == 0;
    }

    @Override
    public boolean hasConfigurationPathArgument() {
        return args[0].compareToIgnoreCase("-c") == 0 || args[0].compareToIgnoreCase("--config") == 0;
    }

    @Override
    public boolean invalidArguments() {
        if (!hasAllRequiredArguments) printInvalid();
        return !hasAllRequiredArguments;
    }

    @Override
    public void printUsage() {
        System.out.println(usageLine());
        System.out.println(description());
        System.out.println();
        System.out.println("Options:");
        for (String option : listOptions()) {
            System.out.println(option);
        }
        System.out.println();
        System.out.println(example());
    }

    protected void printInvalid() {
        System.out.println("Invalid arguments!");
        System.out.println();
    }

}
