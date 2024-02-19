package edu.cut.smacc.test.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.SMACCObject;
import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.minio.MinioFileReader;
import edu.cut.smacc.server.minio.MinioFileWriter;
import edu.cut.smacc.server.minio.MinioManager;
import edu.cut.smacc.server.tier.CacheOutputStream;
import edu.cut.smacc.server.tier.TierManager;
import edu.cut.smacc.server.tier.result.GetResult;
import edu.cut.smacc.server.tier.result.PutResult;

/**
 * Main tests for the MinIO manager
 */
public class TestMinioManager {

    public static void main(String[] args) throws IOException {
        String bucket = "smacc";
        String endPoint = "http://127.0.0.1:9000";
        String region = "";
        String accessKey = "b3duP8c3BoNjJZWmf0SM";
        String secretKey = "BPl7uFQFApWNbM9fuTqXBB9zyRpKdwUSoHKxBGHu";
        CloudInfo cloudInfo = new CloudInfo(endPoint, region, accessKey, secretKey);

        Configuration conf = new Configuration();
        conf.addProperty(ServerConfigurations.BACKEND_CLOUD_STORAGE_KEY, "MinIO");
        conf.addProperty(ServerConfigurations.S3_DEFAULT_BUCKET_KEY, bucket);
        conf.addProperty(ServerConfigurations.S3_AMAZON_ENDPOINT_KEY, endPoint);
        conf.addProperty(ServerConfigurations.S3_DEFAULT_REGION_KEY, region);
        conf.addProperty(ServerConfigurations.S3_TIER_MASTER_ACCESS_KEY_KEY, accessKey);
        conf.addProperty(ServerConfigurations.S3_TIER_MASTER_SECRET_KEY_KEY, secretKey);
        conf.addProperty(ServerConfigurations.CACHE_DISK_VOLUMES_SIZE_KEY, "0");

        Path tempPath = Files.createTempDirectory("smaccmem");
        conf.addProperty(ServerConfigurations.CACHE_MEMORY_STATE_KEY, tempPath.toFile().getAbsolutePath());

        ServerConfigurations.initialize(conf);

        TestSimpleMinioManager(bucket, cloudInfo);
        TestTierMinioManager(bucket, cloudInfo, conf, false);
        TestTierMinioManager(bucket, cloudInfo, conf, true);

        Files.walk(tempPath).forEach(path -> {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
            }
        });

        System.exit(0);
    }

    private static void TestSimpleMinioManager(String bucket, CloudInfo cloudInfo) throws IOException {

        MinioManager minioMgr = new MinioManager();

        // Create and write a file
        boolean async = false;
        String key = "hero1.txt";
        String content = "This is the content";
        byte[] bytes = content.getBytes();

        System.out.println("Creating file " + bucket + ":" + key);
        MinioFileWriter fileWriter = minioMgr.create(async, bucket, key, Long.valueOf(bytes.length), cloudInfo);

        fileWriter.write(bytes);
        while (fileWriter.isUploadingWithLength()) {
            try {
                fileWriter.waitUploadWithLength(100);
            } catch (InterruptedException e) {
            }
        }
        fileWriter.completeFile();
        fileWriter.close();

        // Read the file
        System.out.println("Reading file " + bucket + ":" + key);
        MinioFileReader fileReader = minioMgr.read(bucket, key, cloudInfo);

        byte[] buffer = new byte[100];
        int r = fileReader.read(buffer);
        fileReader.close();
        content = new String(buffer, 0, r);

        // Read part of the file
        System.out.println("Reading partial file " + bucket + ":" + key);
        fileReader = minioMgr.read(bucket, key, 5, 10, cloudInfo);
        r = fileReader.read(buffer);
        fileReader.close();
        content = new String(buffer, 0, r);

        // Stat the file
        CloudFile file = minioMgr.statFile(bucket, key, cloudInfo);
        System.out.println("Stat file " + file);

        // List the files
        List<CloudFile> files = minioMgr.list(bucket, key, cloudInfo);
        for (CloudFile f : files) {
            System.out.println("List file " + f);
        }

        // Delete the file
        System.out.println("Deleting file " + bucket + ":" + key);
        minioMgr.delete(bucket, key, cloudInfo);

        System.out.println("Completed TestSimpleMinioManager");
    }

    private static void TestTierMinioManager(String bucket, CloudInfo cloudInfo, Configuration conf, boolean async)
            throws IOException {

        TierManager tierMgr = new TierManager(conf);

        // Create and write a file
        String key = "hero1.txt";
        String content = "This is the content";
        byte[] bytes = content.getBytes();

        System.out.println("Creating file " + bucket + ":" + key);
        PutResult putResult = tierMgr.create(bucket, key, async, Long.valueOf(bytes.length), cloudInfo);

        CacheOutputStream out = putResult.getCacheOutputStream();

        out.write(bytes);
        if (!async) {
            while (out.isUploadingWithLength()) {
                try {
                    out.waitUploadWithLength(100);
                } catch (InterruptedException e) {
                }
            }
        }
        out.close();

        // Read the file
        System.out.println("Reading file " + bucket + ":" + key);
        GetResult getResult = tierMgr.read(bucket, key, cloudInfo);
        InputStream in = getResult.getInputStream();

        byte[] buffer = new byte[100];
        int r = in.read(buffer);
        in.close();
        content = new String(buffer, 0, r);

        // Deleting file from cache
        if (!async) {
            System.out.println("Deleting file from cache " + bucket + ":" + key);
            tierMgr.deleteFileFromCache(bucket, key);
        }

        // Read part of the file
        System.out.println("Reading partial file " + bucket + ":" + key);
        getResult = tierMgr.read(bucket, key, 5, 10, cloudInfo);
        in = getResult.getInputStream();
        r = in.read(buffer);
        in.close();
        content = new String(buffer, 0, r);

        // Stat the file
        SMACCObject smaccObj = tierMgr.getSMACCObject(bucket, key);
        System.out.println("Stat cache file " + smaccObj);

        // List the files
        List<SMACCObject> smaccObjs = tierMgr.list(bucket, null);
        for (SMACCObject f : smaccObjs) {
            System.out.println("List cache file " + f);
        }

        // Delete the file
        System.out.println("Deleting file " + bucket + ":" + key);
        tierMgr.delete(bucket, key, cloudInfo);

        System.out.println("Completed TestTierMinioManager");
    }

}
