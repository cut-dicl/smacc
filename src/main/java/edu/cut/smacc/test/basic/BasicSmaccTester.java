package edu.cut.smacc.test.basic;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;

import edu.cut.smacc.client.SMACCClient;
import edu.cut.smacc.configuration.ClientConfigurations;
import edu.cut.smacc.server.cache.common.SMACCObject;

public class BasicSmaccTester {

    private static String CLIENT_CONFIGURATION_PATH = "conf/client.config.properties";

    public static void main(String[] args) throws Exception {
        File confFile = new File(CLIENT_CONFIGURATION_PATH);
        if (!confFile.exists()) {
            throw new Exception("Configuration file not found");
        }

        long v = System.currentTimeMillis();
        SMACCClient.setConfigurationPath(CLIENT_CONFIGURATION_PATH);
        SMACCClient smaccClient = new SMACCClient();
        String bucket = ClientConfigurations.getDefaultBucket();
        System.out.println("Init client: " + (System.currentTimeMillis() - v));

        // PUT string
        v = System.currentTimeMillis();
        String data = "woohoo";
        put(smaccClient, bucket, "testFile", data.getBytes());
        System.out.println("PUT string " + data + " in " + (System.currentTimeMillis() - v));

        // GET string
        v = System.currentTimeMillis();
        byte[] bytes = get(smaccClient, bucket, "testFile");
        String result = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("GET string " + result + " in " + (System.currentTimeMillis() - v));

        // PUT string
        v = System.currentTimeMillis();
        data = "woohoomore";
        put(smaccClient, bucket, "testFile", data.getBytes());
        System.out.println("PUT string " + data + " in " + (System.currentTimeMillis() - v));

        // GET string
        v = System.currentTimeMillis();
        bytes = get(smaccClient, bucket, "testFile");
        result = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("GET string " + result + " in " + (System.currentTimeMillis() - v));

        // LIST
        v = System.currentTimeMillis();
        list(smaccClient, bucket, "testFile");
        System.out.println("LIST in " + (System.currentTimeMillis() - v));

        // LIST CACHE
        v = System.currentTimeMillis();
        listCache(smaccClient, bucket, "testFile");
        System.out.println("LIST CACHE in " + (System.currentTimeMillis() - v));

        // STATUS
        v = System.currentTimeMillis();
        fileStatus(smaccClient, bucket, "testFile");
        System.out.println("STATUS in " + (System.currentTimeMillis() - v));

        // CLEAR CACHE
        v = System.currentTimeMillis();
        smaccClient.clearCache();
        System.out.println("CLEAR CACHE in " + (System.currentTimeMillis() - v));

        // STATUS
        v = System.currentTimeMillis();
        fileStatus(smaccClient, bucket, "testFile");
        System.out.println("STATUS in " + (System.currentTimeMillis() - v));

        // DEL key
        v = System.currentTimeMillis();
        data = "woohoomore";
        delete(smaccClient, bucket, "testFile");
        System.out.println("DEL key in " + (System.currentTimeMillis() - v));

        // LIST
        v = System.currentTimeMillis();
        list(smaccClient, bucket, "testFile");
        System.out.println("LIST in " + (System.currentTimeMillis() - v));

        // LIST CACHE
        v = System.currentTimeMillis();
        listCache(smaccClient, bucket, "testFile");
        System.out.println("LIST CACHE in " + (System.currentTimeMillis() - v));

        // STATUS
        v = System.currentTimeMillis();
        fileStatus(smaccClient, bucket, "testFile");
        System.out.println("STATUS in " + (System.currentTimeMillis() - v));

        // DEL key
        v = System.currentTimeMillis();
        data = "woohoomore";
        delete(smaccClient, bucket, "testFile");
        System.out.println("DEL key in " + (System.currentTimeMillis() - v));
    }

    private static boolean put(SMACCClient smaccClient, String bucket, String key, byte[] values) {
        try (InputStream input = new ByteArrayInputStream(values)) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(values.length);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, input, metadata);
            try {
                PutObjectResult res = smaccClient.putObject(putObjectRequest);
                if (res.getETag() == null) {
                    return false;
                }
            } catch (Exception e) {
                System.err.println("Not possible to write object :" + key);
                e.printStackTrace();
                return false;
            }
        } catch (Exception e) {
            System.err.println("Error in the creation of the stream :" + e.toString());
            e.printStackTrace();
            return false;
        }

        return true;
    }

    private static byte[] get(SMACCClient smaccClient, String bucket, String key) {
        try {
            S3Object object = getS3ObjectAndMetadata(smaccClient, bucket, key);
            InputStream objectData = object.getObjectContent();
            return IOUtils.toByteArray(objectData);
        } catch (Exception e) {
            System.err.println("Not possible to get the object " + key);
            e.printStackTrace();
            return null;
        }
    }

    private static S3Object getS3ObjectAndMetadata(SMACCClient smaccClient, String bucket, String key) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, key);
        return smaccClient.getObject(getObjectRequest);
    }

    private static boolean delete(SMACCClient smaccClient, String bucket, String key) {
        try {
            boolean result = smaccClient.deleteObject2(bucket, key);
            System.out.println("Key " + key + " deleted: " + result);
        } catch (Exception e) {
            System.err.println("Not possible to delete the key " + key);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private static void list(SMACCClient smaccClient, String bucket, String key) {
        ListObjectsRequest listRequest = new ListObjectsRequest()
                .withBucketName(bucket)
                .withPrefix(key);

        ObjectListing result = smaccClient.listObjects(listRequest);
        List<S3ObjectSummary> objects = new ArrayList<>(result.getObjectSummaries());
        while (result.isTruncated()) {
            result = smaccClient.listNextBatchOfObjects(result);
            objects.addAll(result.getObjectSummaries());
        }

        if (objects.isEmpty()) {
            System.out.println("No objects found in S3");
        } else {
            System.out.println("Objects found in S3:");
            for (S3ObjectSummary obj : objects) {
                System.out.println("Key: " + obj.getKey() + " with size: " + obj.getSize());
            }
        }
    }

    private static void listCache(SMACCClient smaccClient, String bucket, String key) {
        List<SMACCObject> objects = smaccClient.cacheList(bucket, key);
        if (objects.isEmpty()) {
            System.out.println("No objects found in the cache");
        } else {
            System.out.println("Objects found in cache:");
            for (SMACCObject obj : objects) {
                System.out.println("Key: " + obj.getKey() + " with size: " + obj.getActualSize()
                        + " cached in: " + obj.getType());
            }
        }

    }

    private static void fileStatus(SMACCClient smaccClient, String bucket, String key) {
        SMACCObject smaccObject = smaccClient.fileStatusRequest(bucket, key);
        if (smaccObject != null)
            System.out.println(smaccObject.metadataString());
        else
            System.out.println("Key " + key + " not found!");
    }
}
