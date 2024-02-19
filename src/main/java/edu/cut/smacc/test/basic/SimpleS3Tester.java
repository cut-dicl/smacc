package edu.cut.smacc.test.basic;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import edu.cut.smacc.cli.SmaccCLI;
import edu.cut.smacc.configuration.ClientConfigurations;
import edu.cut.smacc.configuration.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class SimpleS3Tester {

    private static String CLIENT_CONFIGURATION_PATH = "conf/client.config.properties";

    public static void main(String[] args) throws Exception {
        // Check for CLI arguments
        SmaccCLI cli = new SimpleS3TesterCLI(args);
        if (cli.hasArguments()) {
            if (cli.hasHelpArgument() || cli.invalidArguments()) {
                System.exit(0);
            }
        }

        File confFile = new File(CLIENT_CONFIGURATION_PATH);
        if (!confFile.exists()) {
            throw new Exception("Configuration file not found");
        }
        Configuration conf = new Configuration(CLIENT_CONFIGURATION_PATH);
        ClientConfigurations.initialize(conf);

        String bucket = ClientConfigurations.getDefaultBucket();
        String key = "aFile";
        File file = new File("cache/file.txt");

        BasicAWSCredentials clientCredentials = new BasicAWSCredentials(ClientConfigurations.getMasterAccessKey(),
                ClientConfigurations.getMasterSecretKey());
        EndpointConfiguration endPointConfiguration = new EndpointConfiguration(
                ClientConfigurations.getS3AmazonEndpoint(), ClientConfigurations.getDefaultRegion());
        AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(clientCredentials))
                .withEndpointConfiguration(endPointConfiguration).build();

        // Put object
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(file.length());
        client.putObject(bucket, key, new FileInputStream(file), meta);

        // Get objects list
        client.listObjects(bucket).getObjectSummaries().forEach(i -> System.out.println(i.getKey()));

        // Read object
        byte[] b = new byte[100];
        InputStream in = client.getObject(bucket, key).getObjectContent();
        System.out.println(in.read(b));
        System.out.println(new String(b));

        // Delete object
        client.deleteObject(bucket, key);

        // Get objects list
        client.listObjects(bucket).getObjectSummaries().forEach(i -> System.out.println(i.getKey()));
    }

    static void setClientConfigurationPath(String path) {
        CLIENT_CONFIGURATION_PATH = path;
    }
}
