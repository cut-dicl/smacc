package edu.cut.smacc.test.internal;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.configuration.BaseConfigurations;
import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.SMACCObject;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.policy.admission.AdmissionPolicy;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import edu.cut.smacc.server.cache.policy.eviction.placement.EvictionPlacementPolicy;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.tier.TierManager;

import java.io.InputStream;
import java.util.List;

/**
 * This class exists only for testing the internals of smacc (tier manager)
 * without the need of network communication
 *
 * @author Theodoros Danos
 */

public class TestCacheSystem {
    private static TierManager tier;

    private static void HandleKeyboardInterrupt() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (tier != null)
                synchronized (tier) {
                    tier.shutdown();
                }
        }));
    }

    public static void main(String[] args) {
        try {

            System.out.println("\t--------- TESTING CACHE SYSTEM ---------\n\n");

            HandleKeyboardInterrupt();

            /* Cache write and save policies to DISK ONLY */
            Configuration conf = new Configuration();
            addCredentials(conf);
            conf.addProperty(ServerConfigurations.UPLOAD_LOCATION_KEY, "DISKONLY");
            conf.addProperty(ServerConfigurations.REQUEST_LOCATION_KEY, "DISKONLY");
            conf.addProperty(ServerConfigurations.CACHE_DISK_VOLUMES_SIZE_KEY, 2);
            conf.addProperty(ServerConfigurations.CACHE_DISK_VOLUME_STRING,
                    "cache/DiskData1/, cache/DiskState1, 1467531800");
            conf.addProperty(ServerConfigurations.CACHE_DISK_VOLUME_STRING + "0",
                    "cache/DiskData1/, cache/DiskState1, 1467531800");
            conf.addProperty(ServerConfigurations.CACHE_DISK_VOLUME_STRING + "1",
                    "cache/DiskData2/, cache/DiskState2/, 1467531800");
            tier = new TierManager(conf);

            BasicAWSCredentials credentials = new BasicAWSCredentials(
                    ServerConfigurations.getMasterAccessKey(),
                    ServerConfigurations.getMasterSecretKey());
            CloudInfo cloudInfo = new CloudInfo(
                    ServerConfigurations.getS3AmazonEndpoint(),
                    ServerConfigurations.getDefaultRegion(),
                    ServerConfigurations.getMasterAccessKey(),
                    ServerConfigurations.getMasterSecretKey());

            String bucket = "smaccbucket";
            Thread t1;

            /*
             * Put an object using the bucket smaccbucket, key file.txt, using as data the contents
             * of file 1MB in a synchronous write.
             */
            System.out.println("PUT ");
            t1 = CreateFile(bucket, "file.txt", "1MB", false, 50000000, credentials,1024000);
            t1.join();

            testEvictionItemPolicy();

            /*
            * Get the object using the bucket smaccbucket, key file3.txt, using as data the contents
            * of file 1MB in a synchronous read.
            * Note: Reading it from the disk
             */
            System.out.println("GET ");
            long readBegin = System.currentTimeMillis();
            InputStream readItem = tier.read(bucket, "file.txt", cloudInfo).getInputStream();
            long readEnd = System.currentTimeMillis();
            if (readItem != null) {
                System.out.println("Disk read was success and it took " + (readEnd - readBegin) + " ms");
                testAdmissionPolicy(conf);
                testEvictionPlacementPolicy();
                testEvictionItemPolicy();

                printList(bucket, tier);
            } else {
                System.out.println("Read was unsuccessful");
            }
            // Put a second item
            Thread t2 = CreateFile(bucket, "file2.txt", "2MB", false, 50000000, credentials,1024000 * 2);
            t2.join();

            testEvictionItemPolicy();

        } catch (Exception e) {
            System.err.println("MAIN_EXCEP:" + e.getMessage());
            e.printStackTrace(System.err);
        }

        if (tier != null)
            tier.shutdown();
    }

    public static void testAdmissionPolicy(Configuration conf) {
        AdmissionPolicy policy = AdmissionPolicy.getInstance(conf);
        if (policy.getReadAdmissionLocation(null) == StoreOptionType.DISK_ONLY &&
                policy.getWriteAdmissionLocation(null) == StoreOptionType.DISK_ONLY) {
            System.out.println("Admission policy is disk only for both read and write");
            return;
        }
        System.err.println("Admission policy is not disk only for both read and write");
    }

    public static void testEvictionPlacementPolicy() {
        EvictionPlacementPolicy policy = tier.getCachePolicyNotifier().getEvictionPlacementPolicy();
        if (policy.downgrade(null)) {
            System.out.println("Eviction placement policy is downgrade");
            return;
        }
        System.err.println("Eviction placement policy is not delete");
    }

    public static void testEvictionItemPolicy() {
        EvictionItemPolicy diskPolicy = tier.getCachePolicyNotifier().getEvictionItemPolicy();
        System.out.println("Disk eviction according to LRU at the time: " +
                diskPolicy.getItemToEvict(StoreOptionType.DISK_ONLY).getKey());

    }

    private static void printList(String bucket, TierManager tier) {
        List<SMACCObject> list = tier.list(bucket, null);
        for (SMACCObject obj : list) {
            System.out.println(obj.getKey() + " " + obj.getType() + " Size: " + obj.getCacheObjectSize());
        }
    }

    private static Thread CreateFile(String bucket, String key, String outfile, boolean async, int bufferSize,
            BasicAWSCredentials clientCredentials, long length) {
        Thread t = new Thread(new CreateFileT(bucket, key, outfile, async, tier, bufferSize, clientCredentials, length));
        t.start();
        return t;
    }

    @SuppressWarnings("unused")
    private static Thread SaveFile(String bucket, String key, String outfile, int buffsize, int msdelay,
            BasicAWSCredentials clientCredentials) {
        Thread t = new Thread(new SaveFileT(bucket, key, outfile, buffsize, msdelay, tier, clientCredentials));
        t.start();
        return t;
    }

    static Thread SaveFile(String bucket, String key, String outfile, int buffsize, int msdelay, long start,
            long stop, BasicAWSCredentials clientCredentials) {
        Thread t = new Thread(
                new SaveFileT(bucket, key, outfile, buffsize, msdelay, tier, start, stop, clientCredentials));
        t.start();
        return t;
    }

    private static void addCredentials(Configuration conf) {
        conf.addProperty(BaseConfigurations.S3_TIER_MASTER_ACCESS_KEY_KEY, "AKIAUXFSXXJJSWFAL3ED");
        conf.addProperty(BaseConfigurations.S3_TIER_MASTER_SECRET_KEY_KEY, "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");
        conf.addProperty(BaseConfigurations.S3_DEFAULT_REGION_KEY, "eu-central-1");
        conf.addProperty(BaseConfigurations.S3_DEFAULT_BUCKET_KEY, "smaccbucket");
        conf.addProperty(ServerConfigurations.SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT_KEY, "https://35bb-46-251-115-185.eu.ngrok.io");
    }
}