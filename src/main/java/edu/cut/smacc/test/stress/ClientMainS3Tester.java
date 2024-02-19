package edu.cut.smacc.test.stress;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;

/**
 * A test2 class for using direct s3 client for repetitive requests
 *
 * @author Theodoros Danos
 */
public class ClientMainS3Tester {

    private static DataOutputStream report;

    public static void main(String[] args) {

        if (args.length < 3) {
            System.out.println("Usage: ./client.jar action[put|get] filename[load|save] iterations[num]");
            System.exit(1);
        }

        try {
            Configuration conf = new Configuration("conf/server.config.properties");
            ServerConfigurations.initialize(conf);

            String bucket = ServerConfigurations.getDefaultBucket();
            String action = args[0].toUpperCase();
            String filename = args[1];
            int iterations = Integer.parseInt(args[2]);

            if (!action.equals("PUT") && !action.equals("GET")) {
                System.out.println("Action " + action + " not recognized");
                System.exit(1);
            }

            // prologue
            report = new DataOutputStream(new FileOutputStream(new File("reports.out")));
            StringBuilder repHeader = new StringBuilder();
            repHeader.append("Action: ").append(action).append("\n");
            repHeader.append("Filename: ").append(filename).append("\n");
            repHeader.append("Records: ").append(iterations).append("\n");
            repHeader.append("---\n");
            report.write(repHeader.toString().getBytes());

            BasicAWSCredentials clientCredentials = new BasicAWSCredentials(ServerConfigurations.getMasterAccessKey(),
                    ServerConfigurations.getMasterSecretKey());
            EndpointConfiguration endPointConfiguration = new EndpointConfiguration(
                    ServerConfigurations.getS3AmazonEndpoint(), ServerConfigurations.getDefaultRegion());
            AmazonS3 client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(clientCredentials))
                    .withEndpointConfiguration(endPointConfiguration).build();

            if (action.equals("PUT")) {
                long sum = 0;
                for (int i = 0; i < iterations; ++i) {
                    long start = System.nanoTime();
                    // PUT DATA
                    File file = new File(filename);
                    ObjectMetadata meta = new ObjectMetadata();
                    meta.setContentLength(file.length());
                    InputStream input = new FileInputStream(file);
                    client.putObject(bucket, "SMACCBENCH_" + i, input, meta);
                    long stop = System.nanoTime();
                    sum += stop - start;
                    record(i);
                }

                // Epilogue
                repHeader = new StringBuilder();
                repHeader.append("---\n");
                repHeader.append("Average(ns): ").append(sum / (float) iterations).append("\n");
                report.write(repHeader.toString().getBytes());
            } else {
                // List
                long sum = 0;
                for (int i = 0; i < iterations; ++i) {
                    long start = System.nanoTime();
                    // GET

                    GetObjectRequest req = new GetObjectRequest(bucket, "SMACCBENCH_" + i);
                    S3Object object = client.getObject(req);
                    InputStream stream = object.getObjectContent();

                    byte[] barray = new byte[1000000];

                    int r = stream.read(barray);
                    while (r > 0) {
                        r = stream.read(barray);
                    }
                    stream.close();
                    record(i);

                    long stop = System.nanoTime();
                    sum += stop - start;
                    record(i);
                }
                // Epilogue
                repHeader = new StringBuilder();
                repHeader.append("---\n");
                repHeader.append("Average(ns): ").append(sum / (float) iterations).append("\n");
                report.write(repHeader.toString().getBytes());
            }

            report.flush();
            report.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void record(int i) throws IOException {
        report.write(String.valueOf(System.nanoTime()).getBytes());
        String v = " " + i + "\n";
        report.write(v.getBytes());
    }
}
