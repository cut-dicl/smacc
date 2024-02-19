package edu.cut.smacc.test.basic;

import com.amazonaws.services.s3.model.ObjectMetadata;

import edu.cut.smacc.cli.SmaccCLI;
import edu.cut.smacc.configuration.ClientConfigurations;
import edu.cut.smacc.client.SMACCClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;

public class SimpleSmaccTester {

    private static String CLIENT_CONFIGURATION_PATH = "conf/client.config.properties";

    public static void main(String[] args) throws Exception {
        // Check for CLI arguments
        SmaccCLI cli = new SimpleSmaccTesterCLI(args);
        if (cli.hasArguments()) {
            if (cli.hasHelpArgument() || cli.invalidArguments()) {
                System.exit(0);
            }
        }

        File confFile = new File(CLIENT_CONFIGURATION_PATH);
        if (!confFile.exists()) {
            throw new Exception("Configuration file not found");
        }
        SMACCClient.setConfigurationPath(CLIENT_CONFIGURATION_PATH);
        long v = System.currentTimeMillis();
        SMACCClient client = new SMACCClient();
        System.out.println("Init client in " + (System.currentTimeMillis() - v));

        String bucket = ClientConfigurations.getDefaultBucket();

        // PUT string
        v = System.currentTimeMillis();
        client.putObject(bucket, "testFile", "wohoo");
        System.out.println("PUT string in " + (System.currentTimeMillis() - v));

        // PUT file
        v = System.currentTimeMillis();
        boolean fileCreated = false;
        File file = new File("cache/file.txt");
        if (!file.exists()) {
            FileOutputStream fout = new FileOutputStream(file);
            fout.write("test file".getBytes(Charset.defaultCharset()));
            fout.write(System.lineSeparator().getBytes(Charset.defaultCharset()));
            fout.flush();
            fout.close();
            fileCreated = true;
        }

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(file.length());
        client.putObject(bucket, file.getName(), file);
        System.out.println("PUT file in " + (System.currentTimeMillis() - v));

        client.cacheList(bucket).forEach(i -> System.out.println(i.getKey() + "  " + i.getType()));

        // GET
        InputStream stream = client.getObject(bucket, "file.txt").getObjectContent();
        byte[] bytes = new byte[1000];
        int r = stream.read(bytes);
        while (r > 0) {
            System.err.println(new String(bytes, 0, r));
            r = stream.read(bytes);
        }
        stream.close();

        client.cacheList(bucket).forEach(i -> System.out.println(i.getKey() + "  " + i.getType()));

        v = System.currentTimeMillis();
        client.deleteObject(bucket, "file.txt");
        System.out.println("Delete file in " + (System.currentTimeMillis() - v));

        System.out.println("List size: " + client.cacheList(bucket).size());

        if (fileCreated) {
            file.delete();
        }
    }

    static void setClientConfigurationPath(String path) {
        CLIENT_CONFIGURATION_PATH = path;
    }
}
