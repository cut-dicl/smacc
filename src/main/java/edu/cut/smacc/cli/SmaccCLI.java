package edu.cut.smacc.cli;

import java.util.ArrayList;

/**
 * SmaccCLI represents a basic template for the SMACC command-line interfaces.
 * It provides simple methods for parsing command-line arguments, checking for required options, and
 * generating usage information.
 */
public interface SmaccCLI {

    /**
     * Checks if the command-line has any arguments.
     *
     * @return true if there are command-line arguments, false otherwise.
     */
    boolean hasArguments();

    /**
     * Parses the command-line arguments and initializes the configuration based on the provided options.
     */
    void parseArguments();

    /**
     * Checks if the "help" argument is present in the command-line arguments.
     *
     * @return true if the "help" argument is present, false otherwise.
     */
    boolean hasHelpArgument();

    /**
     * Checks if the "configuration path" argument is present in the command-line arguments.
     *
     * @return true if the "configuration path" argument is present, false otherwise.
     */
    boolean hasConfigurationPathArgument();

    /**
     * Checks if there are missing required options in the command-line arguments.
     *
     * @return true if there are missing required options, false otherwise.
     */
    boolean invalidArguments();

    /**
     * Provides a brief description of the interface and its purpose.
     *
     * @return a string describing the purpose of the interface.
     */
    String description();

    /**
     * Provides a usage line that illustrates how to use the interface.
     *
     * @return a string representing the usage line of the interface.
     */
    String usageLine();

    /**
     * Lists all the available options for the interface.
     *
     * @return an ArrayList containing the available options as strings.
     */
    ArrayList<String> listOptions();

    /**
     * Provides an example of how to use the interface with command-line arguments.
     *
     * @return a string representing an example of using the interface.
     */
    String example();

    /**
     * Prints the usage information of the SmaccCLI interface to the standard output.
     */
    void printUsage();
}
