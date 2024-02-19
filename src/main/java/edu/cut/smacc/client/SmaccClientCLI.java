package edu.cut.smacc.client;

import edu.cut.smacc.cli.SmaccCLIBase;
import edu.cut.smacc.configuration.ClientConfigurations;
import edu.cut.smacc.server.cache.common.SMACCObject;
import edu.cut.smacc.server.protocol.RequestType;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * A CLI that exposes a simple API for the client interacting with the SMACC server.
 */
public class SmaccClientCLI extends SmaccCLIBase {

    private static final String OPTION_HELP = """
    -h, --help                     Display this usage information""";

    private static final String OPTION_CONFIG = """
    -c, --config <conf_file>       Specify the path to the client configuration file""";

    private static final String OPTION_PUT_REQUEST = """
    -p, --put <src> <dest>         Request the SMACC server to upload a file or directory to storage""";

    private static final String OPTION_GET_REQUEST = """
    -g, --get <src> <dest>         Request the SMACC server to retrieve an object or a directory of objects from storage, by giving its key""";

    private static final String OPTION_DEL_REQUEST = """
    -d, --del <file/dir>           Request the SMACC server to delete an object or a directory of objects from storage, by giving its key""";

    private static final String OPTION_DEL_CACHE = """
    -dc, --del-cache <file/dir>    Request the SMACC server to delete a an object or a directory of objects from cache, by giving its key""";

    private static final String OPTION_LIST_REQUEST = """
    -l, --list [file/dir]          Request the SMACC server to list object key-value pairs""";

    private static final String OPTION_LIST_CACHE = """
    -lc, --list-cache [file/dir]   Request the SMACC server to list cached object key-value pairs""";

    private static final String OPTION_FILE_STATUS = """
    -fs, --file-status <file>      Print the status of a specific file on SMACC storage""";

    private static final String OPTION_RESET_STATS = """
    -rs, --reset-stats             Request the SMACC server to reset its statistics""";

    private static final String OPTION_COLLECT_STATS = """
    -cs, --collect-stats           Request the collection of SMACC server statistics""";

    private static final String OPTION_CLEAR_CACHE = """
    -cc, --clear-cache             Request the SMACC server to clear the cache""";

    private static final String OPTION_SHUTDOWN = """
    -x, --shutdown                 Request the server to shut down""";
    // Longest amount of arguments are -p file.txt s3/dir/myfile.txt and -g s3/dir/file.txt myfile.txt with -c conf.properties -p file.txt
    private static final int ARGUMENTS_MAX_LENGTH = 5;


    private RequestType requestType;

    private String operationFileKey;
    private String destinationFileKey;
    private String configurationPath;

    public SmaccClientCLI(String[] args) {
        super(args);
    }

    public static void main(String[] args) {
        SmaccClientCLI clientCLI = new SmaccClientCLI(args);
        if (clientCLI.hasArguments()) {
            if (clientCLI.hasHelpArgument() || clientCLI.invalidArguments()) {
                clientCLI.printUsage();
                System.exit(0);
            }
            handleCommandLineInput(clientCLI);
        } else {
            clientCLI.printUsage();
        }
    }

    private static void handleCommandLineInput(SmaccClientCLI clientCLI) {
        RequestType type = clientCLI.getRequestType();
        String sourceFileKey = clientCLI.getOperationFileKey();
        String destinationFileKey = clientCLI.getDestinationFileKey();

        SMACCClient client = new SMACCClient();
        String bucket = ClientConfigurations.getDefaultBucket();
        switch (type) {
            case PUT -> {
                File fileToPut = new File(sourceFileKey);
                if (fileToPut.isDirectory()) {
                    client.putDirectoryObject(bucket, sourceFileKey, destinationFileKey);
                } else {
                    client.putObject(bucket, new File(sourceFileKey), destinationFileKey);
                }
            }
            case GET -> {
                client.getDirectoryObject(bucket, sourceFileKey, destinationFileKey);
            }
            case DEL -> {
                try {
                    boolean result = client.deleteObject2(bucket, sourceFileKey);
                    System.out.println("Key " + sourceFileKey + " deleted: " + result);
                } catch (Exception e) {
                    System.err.println("Not possible to delete the key " + sourceFileKey);
                    e.printStackTrace();
                }
            }
            case DEL_CACHE -> {
                try {
                    boolean result = client.deleteCacheObject(bucket, sourceFileKey);
                    System.out.println("Key " + sourceFileKey + " deleted from cache: " + result);
                } catch (Exception e) {
                    System.err.println("Not possible to delete the key " + sourceFileKey);
                    e.printStackTrace();
                }
            }
            case LIST -> {
                List<S3ObjectSummary> dirObjects = client.listDirectoryObjects(bucket, sourceFileKey);
                System.out.println(dirObjects.size() + " objects found in " + bucket + ":" + sourceFileKey);
                for (S3ObjectSummary obj : dirObjects) {
                    System.out.println(" - " + obj.getKey() + ", size: " + obj.getSize() + ", mod: "
                            + obj.getLastModified());
                }
            }
            case LIST_CACHE -> {
                List<SMACCObject> smaccObjects = client.cacheList(bucket, sourceFileKey);
                System.out.println(smaccObjects.size() + " SMACC cache objects found");
                for (SMACCObject obj : smaccObjects) {
                    String lastMod = (obj.getLastModified() > 0) ? new Date(obj.getLastModified()).toString()
                            : "Unknown";
                    System.out.println(" - " + obj.getKey() + ", size: " + obj.getActualSize() + ", mod: "
                            + lastMod + ", tiers: " + obj.getStoreOptionType());
                }
            }
            case COLLECT_STATS -> client.statsRequest();
            case RESET_STATS -> client.resetStatsRequest();
            case SHUTDOWN -> client.shutdownRequest();
            case CLEAR_CACHE -> client.clearCache();
            case FILE_STATUS -> {
                SMACCObject smaccObject = client.fileStatusRequest(bucket, sourceFileKey);
                if (smaccObject != null)
                    System.out.println(smaccObject.metadataString());
                else
                    System.out.println("Key " + sourceFileKey + " not found!");
            }
        }
    }


    @Override
    public void parseArguments() {
        if (hasHelpArgument()) {
            return;
        }
        if (args.length > ARGUMENTS_MAX_LENGTH) {
            hasAllRequiredArguments = false;
            return;
        }
        ArgumentParser argumentParser = new ArgumentParser();
        argumentParser.parseArguments();
        if (hasNonExistingArguments) {
            hasAllRequiredArguments = false;
            return;
        }

        if (hasOperationArgument() && operationFileKey == null) {
            hasAllRequiredArguments = false;
        }
        if (hasDestinationArgument() && (operationFileKey == null || destinationFileKey == null)) {
            hasAllRequiredArguments = false;
        }
    }

    public boolean hasOperationArgument() {
        return getRequestType() == RequestType.DEL ||
                getRequestType() == RequestType.DEL_CACHE ||
                getRequestType() == RequestType.FILE_STATUS;
    }

    public boolean hasDestinationArgument() {
        return getRequestType() == RequestType.PUT ||
                getRequestType() == RequestType.GET;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public String getOperationFileKey() {
        return operationFileKey;
    }

    public String getDestinationFileKey() {
        return destinationFileKey;
    }

    @Override
    public boolean hasConfigurationPathArgument() {
        return super.hasConfigurationPathArgument() && configurationPath != null;
    }

    @Override
    public String description() {
        return """
                SMACCClient - The main client application for starting SMACC client.
                
                This CLI supports the client configuration file insertion and one of the following operations at a time.""";
    }

    @Override
    public String usageLine() {
        return "Usage: java -cp <path_to_jar>/smacc-2.0-jar-with-dependencies.jar <SMACCClientCLIClass> [options]";
    }

    @Override
    public ArrayList<String> listOptions() {
        return new ArrayList<>() {{
            add(OPTION_HELP);
            add(OPTION_CONFIG);
            add(OPTION_PUT_REQUEST);
            add(OPTION_GET_REQUEST);
            add(OPTION_DEL_REQUEST);
            add(OPTION_DEL_CACHE);
            add(OPTION_LIST_REQUEST);
            add(OPTION_LIST_CACHE);
            add(OPTION_FILE_STATUS);
            add(OPTION_COLLECT_STATS);
            add(OPTION_RESET_STATS);
            add(OPTION_CLEAR_CACHE);
            add(OPTION_SHUTDOWN);
        }};
    }

    @Override
    public String example() {
        return """
                Example: java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.main.client.SmaccClientCLI -c conf/client.config.properties -p ./local/path/file.txt s3bucket/path/myfile.txt

                *If a configuration file is provided, pass it as the first argument.
                **When using an option with S3 directory, make sure to end the directory path with a slash (e.g. path/to/dir/).""";
    }


    /**
     * Helper class that parses the arguments passed to the SMACC Client CLI and
     * assigns appropriate options based on the provided arguments.
     */
    private class ArgumentParser {

        private final Queue<String> argQueue;

        private ArgumentParser() {
            this.argQueue = new LinkedList<>();
            argQueue.addAll(Arrays.asList(args));
        }

        private void parseArguments() {
            while (!argQueue.isEmpty()) {
                parseArgument(argQueue.poll());
            }
        }

        private void parseArgument(String arg) {
            if (arg.compareToIgnoreCase("-c") == 0 || arg.compareToIgnoreCase("--config") == 0) {
                assignConfigurationPathArgument();
            } else if (arg.compareToIgnoreCase("-p") == 0 || arg.compareToIgnoreCase("--put") == 0) {
                assignPutArgument();
            } else if (arg.compareToIgnoreCase("-g") == 0 || arg.compareToIgnoreCase("--get") == 0) {
                assignGetArgument();
            } else if (arg.compareToIgnoreCase("-d") == 0 || arg.compareToIgnoreCase("--del") == 0) {
                assignDelArgument();
            } else if (arg.compareToIgnoreCase("-dc") == 0 || arg.compareToIgnoreCase("--del-cache") == 0) {
                assignDelCacheArgument();
            } else if (arg.compareToIgnoreCase("-l") == 0 || arg.compareToIgnoreCase("--list") == 0) {
                assignListArgument();
            } else if (arg.compareToIgnoreCase("-lc") == 0 || arg.compareToIgnoreCase("--list-cache") == 0) {
                assignListCacheArgument();
            } else if (arg.compareToIgnoreCase("-cs") == 0 || arg.compareToIgnoreCase("--collect-stats") == 0) {
                assignStatsArgument();
            } else if (arg.compareToIgnoreCase("-rs") == 0 || arg.compareToIgnoreCase("--reset-stats") == 0) {
                assignResetStatsArgument();
            } else if (arg.compareToIgnoreCase("-cc") == 0 || arg.compareToIgnoreCase("--clear-cache") == 0) {
                assignClearCacheArgument();
            } else if (arg.compareToIgnoreCase("-x") == 0 || arg.compareToIgnoreCase("--shutdown") == 0) {
                assignShutdownArgument();
            } else if (arg.compareToIgnoreCase("-fs") == 0 || arg.compareToIgnoreCase("--file-status") == 0) {
                assignFileStatusArgument();
            } else {
                hasNonExistingArguments = true;
            }
        }

        private void assignConfigurationPathArgument() {
            if (argQueue.isEmpty()) {
                hasAllRequiredArguments = false;
                return;
            }
            configurationPath = argQueue.poll();
            SMACCClient.setConfigurationPath(configurationPath);
            hasAllRequiredArguments = true;
        }

        private void assignPutArgument() {
            assignRequestType(edu.cut.smacc.server.protocol.RequestType.PUT);
            operationFileKey = argQueue.poll();
            destinationFileKey = argQueue.poll();
        }

        private void assignGetArgument() {
            assignRequestType(edu.cut.smacc.server.protocol.RequestType.GET);
            operationFileKey = argQueue.poll();
            destinationFileKey = argQueue.poll();
        }

        private void assignDelArgument() {
            assignRequestType(edu.cut.smacc.server.protocol.RequestType.DEL);
            operationFileKey = argQueue.poll();
        }

        private void assignDelCacheArgument() {
            assignRequestType(RequestType.DEL_CACHE);
            operationFileKey = argQueue.poll();
        }

        private void assignListArgument() {
            assignRequestType(edu.cut.smacc.server.protocol.RequestType.LIST);
            operationFileKey = argQueue.poll();
        }

        private void assignListCacheArgument() {
            assignRequestType(RequestType.LIST_CACHE);
            operationFileKey = argQueue.poll();
        }

        private void assignStatsArgument() {
            assignRequestType(edu.cut.smacc.server.protocol.RequestType.COLLECT_STATS);
        }

        private void assignResetStatsArgument() {
            assignRequestType(edu.cut.smacc.server.protocol.RequestType.RESET_STATS);
        }

        private void assignClearCacheArgument() {
            assignRequestType(edu.cut.smacc.server.protocol.RequestType.CLEAR_CACHE);
        }

        private void assignShutdownArgument() {
            assignRequestType(edu.cut.smacc.server.protocol.RequestType.SHUTDOWN);
        }

        private void assignFileStatusArgument() {
            assignRequestType(edu.cut.smacc.server.protocol.RequestType.FILE_STATUS);
            operationFileKey = argQueue.poll();
        }

        private void assignRequestType(edu.cut.smacc.server.protocol.RequestType type) {
            if (requestType != null) {
                hasAllRequiredArguments = false;
                return;
            }
            requestType = type;
            hasAllRequiredArguments = true;
        }

    }
}
