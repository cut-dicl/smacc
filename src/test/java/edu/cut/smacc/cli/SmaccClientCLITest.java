package edu.cut.smacc.cli;

import edu.cut.smacc.client.SmaccClientCLI;
import edu.cut.smacc.server.protocol.RequestType;
import org.junit.jupiter.api.Test;

public class SmaccClientCLITest {

    @Test
    void testHasConfigurationPathArgument() {
        // -c and --config with the path should arg should make hasConfigurationPathArgument() return true
        String[] args = new String[]{"-c", "conf/server.config.properties"};
        SmaccClientCLI serverCLI = new SmaccClientCLI(args);
        boolean hasConfigPathArg = serverCLI.hasConfigurationPathArgument();
        assert hasConfigPathArg;
        args = new String[]{"--config", "conf/server.config.properties"};
        serverCLI = new SmaccClientCLI(args);
        hasConfigPathArg = serverCLI.hasConfigurationPathArgument();
        assert hasConfigPathArg;

        // -c and --config without the path should arg should make hasConfigurationPathArgument() return false
        args = new String[]{"-c"};
        serverCLI = new SmaccClientCLI(args);
        hasConfigPathArg = serverCLI.hasConfigurationPathArgument();
        assert !hasConfigPathArg;

        args = new String[]{"--config"};
        serverCLI = new SmaccClientCLI(args);
        hasConfigPathArg = serverCLI.hasConfigurationPathArgument();
        assert !hasConfigPathArg;

        System.out.println("SMACCClientCLITest.testHasConfigurationPathArgument() passed!");
    }

    @Test
    void testHasStatsArgument() {
        // Create a SMACCClientCLI object without arguments
        String[] args = new String[]{};
        SmaccClientCLI serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() != RequestType.COLLECT_STATS;

        // Create a SMACCClientCLI object with help argument
        args = new String[]{"-h"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() != RequestType.COLLECT_STATS;

        // Create a SMACCClientCLI object with only a configuration argument
        args = new String[]{"-c", "conf/server.config.properties"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() != RequestType.COLLECT_STATS;

        // Create a SMACCClientCLI object with stat argument
        args = new String[]{"-cs"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() == RequestType.COLLECT_STATS;

        // Create a SMACCCLientCLI object with --stat argument
        args = new String[]{"--collect-stats"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() == RequestType.COLLECT_STATS;

        // Create a SMACCClientCLI object with stat argument and other arguments
        args = new String[]{"-cs", "-c", "conf/server.config.properties"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() == RequestType.COLLECT_STATS;

        System.out.println("SMACCClientCLITest.testHasStatArgument() passed!");
    }

    @Test
    void testHasListArgument() {
        // Create a SMACCClientCLI object without arguments
        String[] args = new String[]{};
        SmaccClientCLI serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() != RequestType.LIST;

        // Create a SMACCClientCLI object with help argument
        args = new String[]{"-h"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() != RequestType.LIST;

        // Create a SMACCClientCLI object with only a configuration argument
        args = new String[]{"-c", "conf/server.config.properties"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() != RequestType.LIST;

        // Create a SMACCClientCLI object with list argument
        args = new String[]{"-l"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() == RequestType.LIST;

        // Create a SMACCCLientCLI object with --list argument
        args = new String[]{"--list"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() == RequestType.LIST;

        args = new String[]{"-l", "dir"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() == RequestType.LIST;

        args = new String[]{"-c", "conf/server.config.properties", "-l"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.getRequestType() == RequestType.LIST;

        System.out.println("SMACCClientCLITest.testHasListArgument() passed!");
    }

    @Test
    void testHasOperationArgument() {
        // Create a SMACCClientCLI object without arguments
        String[] args = new String[]{};
        SmaccClientCLI serverCLI = new SmaccClientCLI(args);
        assert !serverCLI.hasOperationArgument();

        // Create a SMACCClientCLI object with help argument
        args = new String[]{"-h"};
        serverCLI = new SmaccClientCLI(args);
        assert !serverCLI.hasOperationArgument();

        // Create a SMACCClientCLI object with only a configuration argument
        args = new String[]{"-c", "conf/server.config.properties"};
        serverCLI = new SmaccClientCLI(args);
        assert !serverCLI.hasOperationArgument();

        // Create a SMACCClientCLI object with operation argument
        args = new String[]{"-g"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.hasDestinationArgument();
        // However, it is missing the file key
        assert serverCLI.invalidArguments();

        // Create a SMACCCLientCLI object with --operation argument
        args = new String[]{"--get"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.hasDestinationArgument();

        // Create a SMACCClientCLI object with operation argument and other arguments
        args = new String[]{"-p", "-c", "conf/server.config.properties"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.hasDestinationArgument();

        // Reverse of the previous command
        args = new String[]{"-c", "conf/server.config.properties", "-d"};
        serverCLI = new SmaccClientCLI(args);
        assert serverCLI.hasOperationArgument();

        System.out.println("SMACCClientCLITest.testHasOperationArgument() passed!");
    }

    @Test
    void testInvalidArguments() {
        // Adding stats argument or configuration argument should not be missing required options
        String[] args = new String[]{"-cs", "-c", "conf/server.config.properties"};
        SmaccClientCLI clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getOperationFileKey() == null;

        // Same with list argument
        args = new String[]{"-l", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // -l can take -c as a file key, but -cs cannot
        args = new String[]{"-l", "-c"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        args = new String[]{"-cs", "-c"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // Passing -c as a put file is valid
        args = new String[]{"-p", "-c", "conf", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getOperationFileKey().equals("-c");

        // Using -p without a file is invalid
        args = new String[]{"-p", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        // Reverse of the previous command should be valid but -l and -cs will be the name of the configuration files,
        // not the option
        args = new String[]{"-c", "-l"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() != RequestType.LIST;
        args = new String[]{"-c", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() != RequestType.COLLECT_STATS;

        // Not having anything of them should be missing required options
        args = new String[]{};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // Help argument still means missing required options
        args = new String[]{"-h"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-g", "file.txt", "l", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        // Test PUT, GET, DEL without and with file key
        args = new String[]{"-p"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.hasDestinationArgument();
        // Missing file key
        assert clientCLI.getOperationFileKey() == null;
        assert clientCLI.invalidArguments();
        // Missing destination key
        args = new String[]{"-p", "test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.hasDestinationArgument();
        assert clientCLI.getOperationFileKey().equals("test.txt");
        assert clientCLI.getDestinationFileKey() == null;
        assert clientCLI.invalidArguments();
        args = new String[]{"-p", "test.txt", "s3/dest/test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.hasDestinationArgument();
        assert clientCLI.getOperationFileKey().equals("test.txt");
        assert clientCLI.getDestinationFileKey().equals("s3/dest/test.txt");
        assert !clientCLI.invalidArguments();
        args = new String[]{"-g"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.hasDestinationArgument();
        assert clientCLI.invalidArguments();
        args = new String[]{"-g", "test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.hasDestinationArgument();
        assert clientCLI.getOperationFileKey().equals("test.txt");
        assert clientCLI.invalidArguments();
        args = new String[]{"-g", "test.txt", "local/dest/test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.hasDestinationArgument();
        assert clientCLI.getOperationFileKey().equals("test.txt");
        assert clientCLI.getDestinationFileKey().equals("local/dest/test.txt");
        assert !clientCLI.invalidArguments();
        args = new String[]{"-d"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.hasOperationArgument();
        assert clientCLI.getOperationFileKey() == null;
        assert clientCLI.invalidArguments();
        args = new String[]{"-d", "test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.hasOperationArgument();
        assert !clientCLI.invalidArguments();

        // Test multiple operation options at the same time
        args = new String[]{"-p", "-g", "-d"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        args = new String[]{"-p", "-g", "-d", "test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();
        args = new String[]{"-p", "-g", "-d", "test.txt", "test2.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-p", "-g"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // Test multiple operation options at the same time with configuration file
        args = new String[]{"-p", "-g", "-d", "-c"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();
        args = new String[]{"-p", "-g", "-d", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        args = new String[]{"-p", "-g", "-d", "test.txt", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-p", "test.txt", "test2.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        // Operations with no file/dir
        args = new String[]{"-p"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();
        args = new String[]{"-g"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();
        args = new String[]{"-d"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();
        // -l and -cs should work
        args = new String[]{"-l"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        args = new String[]{"-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        // Test -l and -cs with configuration file
        // this is not valid because general listing -l has to be last
        //  instead it will be considered as directory listing and takes
        //  the next argument as the directory key
        args = new String[]{"-l", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();
        args = new String[]{"-cs", "-c"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();
        args = new String[]{"-cs", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() == RequestType.COLLECT_STATS;

        // -cs with file/dir is wrong
        args = new String[]{"-cs", "test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-p", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();
        assert clientCLI.getRequestType() == RequestType.PUT;
        args = new String[]{"-p", "-l"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();
        assert clientCLI.getRequestType() == RequestType.PUT;

        // Operations with file and -cs -l should not work
        args = new String[]{"-p", "test.txt", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        args = new String[]{"-p", "test.txt", "-l"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        args = new String[]{"-p", "test.txt", "-g", "test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // -l operation also supports giving the bucket directory to list
        args = new String[]{"-l", "dir"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        args = new String[]{"-l", "-c"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        // -x or -- shutdown should only work with now arguments, and alone, just like -cs
        args = new String[]{"-x"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() == RequestType.SHUTDOWN;

        args = new String[]{"-x", "-c"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"--shutdown", "test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"--shutdown", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-x", "-l"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // Using it with -c and configuration filename should work
        args = new String[]{"-x", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        // Trying non-existing option
        args = new String[]{"-z"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // -cc or --clear_cache should only work with no other arguments, except conf just like -cs
        args = new String[]{"-cc"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() == RequestType.CLEAR_CACHE;

        args = new String[]{"-cc", "-c"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-cc", "test.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-cc", "-l"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"--cc", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-cc", "-x"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-cc", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        // Reverse and long opt-name
        args = new String[]{"-c", "conf/server.config.properties", "--clear-cache"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() == RequestType.CLEAR_CACHE;

        // CACHE ONLY REQUESTS -dc or --del-cache & -lc or --list-cache
        // Just like the other requests, they cannot be applied with other options, except -c
        args = new String[]{"-dc"};
        clientCLI = new SmaccClientCLI(args);
        // Invalid because it is missing the file key / directory prefix
        assert clientCLI.invalidArguments();
        assert clientCLI.getRequestType() == RequestType.DEL_CACHE;

        args = new String[]{"-dc", "file.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() ==  RequestType.DEL_CACHE;

        args = new String[]{"--del-cache", "file.txt", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        args = new String[]{"-dc", "file.txt", "-c", "conf/server.config.properties", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // -lc now
        args = new String[]{"-lc"};
        clientCLI = new SmaccClientCLI(args);
        // Not invalid because it simply lists the bucket
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() ==  RequestType.LIST_CACHE;

        // In -lc with -c case the -c MUST be specified first
        args = new String[]{"-lc", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-c", "conf/server.config.properties", "--list-cache"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() == RequestType.LIST_CACHE;

        args = new String[]{"-lc", "-c", "conf/server.config.properties", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // -fs or --file-status should only work with a file argument and optionally conf
        args = new String[]{"-fs"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-fs", "file.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();
        assert clientCLI.getRequestType() == RequestType.FILE_STATUS;

        args = new String[]{"-fs", "file.txt", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        args = new String[]{"-fs", "file.txt", "-c", "conf/server.config.properties", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        // One test with --file-status
        args = new String[]{"-c", "conf/server.config.properties", "--file-status", "file.txt"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        // Just like -cs or --collect-stats, -rs or --reset-stats should only work with -c
        args = new String[]{"-rs"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        args = new String[]{"-rs", "-c", "conf/server.config.properties"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        args = new String[]{"-rs", "-c", "conf/server.config.properties", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        args = new String[]{"-c", "conf/server.config.properties", "--reset-stats"};
        clientCLI = new SmaccClientCLI(args);
        assert !clientCLI.invalidArguments();

        args = new String[]{"-c", "conf/server.config.properties", "--reset-stats", "-cs"};
        clientCLI = new SmaccClientCLI(args);
        assert clientCLI.invalidArguments();

        System.out.println("SMACCClientCLITest.testInvalidArguments() passed!");
    }

}
