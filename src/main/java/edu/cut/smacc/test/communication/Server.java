package edu.cut.smacc.test.communication;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import edu.cut.smacc.cli.SmaccCLI;
import edu.cut.smacc.configuration.ClientConfigurations;
import edu.cut.smacc.configuration.Configuration;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    static String SERVER_TEST_CONFIGURATION_PATH = "conf/server.config.properties";
    public static void main(String[] args) throws Exception {
        SmaccCLI cli = new ServerTestCLI(args);
        if (cli.hasArguments()) {
            if (cli.hasHelpArgument() || cli.invalidArguments()) {
                System.exit(0);
            }
        }

        Configuration conf = new Configuration("conf/client.config.properties");
        ClientConfigurations.initialize(conf);

        ServerSocket serverSocket = new ServerSocket(8080);
        Socket socket = serverSocket.accept();
        System.out.println("new connection");
        InputStream in = socket.getInputStream();

        MyInputStream myInputStream = new MyInputStream(512 * 1024);

        Thread t = new Thread(() -> {
            try {
                byte[] bytes = new byte[128 * 1024];

                int x = in.read(bytes);

                while (x != -1) {
                    myInputStream.write(bytes, 0, x);
                    x = in.read(bytes);
                    // Thread.sleep(3000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t.start();
        System.out.println("S3 connection");
        AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        ClientConfigurations.getMasterAccessKey(), ClientConfigurations.getMasterSecretKey())))
                .withEndpointConfiguration(new EndpointConfiguration(ClientConfigurations.getS3AmazonEndpoint(),
                        ClientConfigurations.getDefaultRegion()))
                .build();

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(512 * 1024);

        t.join();

        client.putObject(ClientConfigurations.getDefaultBucket(), "test3.txt", myInputStream, meta);
        System.out.println("S3 closed");
        in.close();
        socket.close();
        System.out.println("connection closed");
        serverSocket.close();
        System.out.println("Server closed");
    }
}
