package edu.cut.smacc.cli;

import edu.cut.smacc.server.main.ServerMainCLI;
import org.junit.jupiter.api.Test;

public class CLIWithConfigurationOnlyTest {

    @Test
    void testHasArguments() {
        // Create a CLI object without arguments
        String[] args = {};
        SmaccCLI serverCLI = new ServerMainCLI(args);
        assert !serverCLI.hasArguments();


        // Create a ServerMainCLI object with arguments
        args = new String[]{"-c", "conf/server.config.properties"};
        serverCLI = new ServerMainCLI(args);
        assert serverCLI.hasArguments();

        System.out.println("ServerMainCLITest.testHasArguments() passed!");
    }

    @Test
    void testHasHelpArgument() {
        // Create a ServerMainCLI object without arguments
        String[] args = new String[]{"-c", "conf/server.config.properties"};
        ServerMainCLI serverCLI = new ServerMainCLI(args);
        assert !serverCLI.hasHelpArgument();

        // Create a ServerMainCLI object with help argument
        args = new String[]{"-h"};
        serverCLI = new ServerMainCLI(args);
        assert serverCLI.hasHelpArgument();
        args = new String[]{"--help"};
        serverCLI = new ServerMainCLI(args);
        assert serverCLI.hasHelpArgument();

        // Create a ServerMainCLI object with -H and --HeLP
        args = new String[]{"-H"};
        serverCLI = new ServerMainCLI(args);
        assert serverCLI.hasHelpArgument();

        args = new String[]{"--HeLP"};
        serverCLI = new ServerMainCLI(args);
        assert serverCLI.hasHelpArgument();

        System.out.println("ServerMainCLITest.testHasHelpArgument() passed!");
    }

    @Test
    void testIsMissingRequiredOptions() {
        // Create a ServerMainCLI object with correct arguments
        String[] args = new String[]{"-c", "conf/server.config.properties"};
        ServerMainCLI serverCLI = new ServerMainCLI(args);
        assert !serverCLI.invalidArguments();

        // Create another with correct arguments
        args = new String[]{"-c", "confff_path"};
        serverCLI = new ServerMainCLI(args);
        assert !serverCLI.invalidArguments();

        // Create one with more arguments that needed
        args = new String[]{"-c", "conf/server.config.properties", "more"};
        serverCLI = new ServerMainCLI(args);
        assert serverCLI.invalidArguments();

        // Create one with fewer arguments that needed
        args = new String[]{"-c"};
        serverCLI = new ServerMainCLI(args);
        assert serverCLI.invalidArguments();

        System.out.println("ServerMainCLITest.testIsMissingRequiredOptions() passed!");
    }

}
