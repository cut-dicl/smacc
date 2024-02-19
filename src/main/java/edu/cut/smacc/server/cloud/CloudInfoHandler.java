package edu.cut.smacc.server.cloud;

import edu.cut.smacc.server.cache.common.StringShort;
import edu.cut.smacc.server.cache.common.crypto.DESDecryptor;
import edu.cut.smacc.server.cache.common.crypto.DESEncryptor;
import edu.cut.smacc.configuration.ServerConfigurations;

import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

/**
 * Handles the continuous saving of new cloud connection information that arise
 * from new buckets in the system
 */
public class CloudInfoHandler implements Runnable {

    private static final Logger logger = LogManager.getLogger(CloudInfoHandler.class);

    /* Instance */
    private final Map<String, CloudInfo> bucketsToCloudInfo;
    private volatile boolean shutdown;
    private Set<String> savedBuckets;

    CloudInfoHandler(Map<String, CloudInfo> bucketToCloudInfo) {
        this.bucketsToCloudInfo = bucketToCloudInfo;
        recoverCredentials(bucketToCloudInfo);

        this.savedBuckets = new HashSet<>();
        this.savedBuckets.addAll(bucketsToCloudInfo.keySet());

        this.shutdown = false;
    }

    void shutdownHandler() {
        shutdown = true;
    }

    public void run() {
        logger.info("Credentials Backup Handler");

        File outFile = new File(ServerConfigurations.getCredentialsLoggerFilename());
        while (!shutdown) {
            synchronized (bucketsToCloudInfo) {
                for (String bucket : bucketsToCloudInfo.keySet()) {
                    if (savedBuckets.contains(bucket))
                        continue; // we already have the bucket saved

                    savedBuckets.add(bucket);

                    CloudInfo cloudInfo = bucketsToCloudInfo.get(bucket);
                    String accessKey = cloudInfo.getAccessKey();
                    String secretKey = cloudInfo.getSecretKey();
                    String endPoint = cloudInfo.getEndPoint();
                    String region = cloudInfo.getRegion();

                    FileOutputStream fout;

                    try {
                        fout = new FileOutputStream(outFile, true); // append to file
                    } catch (FileNotFoundException ex) {
                        logger.error("Unable to locate backup file[" + outFile.getAbsolutePath()
                                + "]. Skipping Credentials: " + ex.getMessage());
                        break;
                    } // LOG

                    JSONObject jsonCred = new JSONObject();
                    jsonCred.put("EndPoint", endPoint);
                    jsonCred.put("Region", region);
                    jsonCred.put("AccessKey", accessKey);
                    jsonCred.put("SecretKey", secretKey);
                    jsonCred.put("Bucket", bucket);
                    String dataOut = jsonCred.toString();
                    String encryptedHex;
                    try {
                        encryptedHex = encrypt(dataOut) + "\n";
                    } catch (Exception ex) {
                        logger.error("Could not encrypt credentials. Skipping Credentials");
                        continue;
                    }

                    try {
                        fout.write(encryptedHex.getBytes());
                        fout.flush();
                        fout.close();
                    } catch (IOException e) {
                        logger.error("Could not backup credentials: " + e.getMessage());
                    } // LOG

                }
            }

            try {
                Thread.sleep(ServerConfigurations.getCredentialsBackupLookupTimeMS());
            } catch (InterruptedException e) {
                /* Do nothing */
            }
        }
    }

    private void recoverCredentials(Map<String, CloudInfo> bucketToCloudInfo) {
        try {
            InputStream in = new FileInputStream((ServerConfigurations.getCredentialsLoggerFilename()));
            Scanner sc = new Scanner(in);
            int c = 0;

            while (sc.hasNextLine()) {
                String encryptedHex = sc.nextLine();
                if (encryptedHex.length() == 0)
                    continue; // Empty line occurred
                String credentialsJsonString = decrypt(encryptedHex);
                if (credentialsJsonString == null) {
                    logger.fatal("Faild to recover credentials: Decryption Failure");
                    continue;
                }
                JSONObject json;
                try {
                    json = new JSONObject(credentialsJsonString);
                } catch (Exception e) {
                    continue;
                } // corrupted string line

                String endPoint = json.getString("EndPoint");
                String region = json.has("Region") ? json.getString("Region") : "";
                String accessKey = json.getString("AccessKey");
                String secretKey = json.getString("SecretKey");
                String bucket = json.getString("Bucket");
                bucketToCloudInfo.put(bucket, new CloudInfo(endPoint, region, accessKey, secretKey));
                c += 1;
            }
            sc.close();

            if (logger.isDebugEnabled())
                logger.info("Recovered Credentials: " + c);

        } catch (IOException e) {
            logger.info("Credentials recovery file not found");
        }
    }

    private static String encrypt(String str) {
        // Encrypt Data -> String (HEX) -> return
        try {

            byte[] initialArray = str.getBytes();
            byte[] endArray;

            ByteArrayOutputStream outfile = new ByteArrayOutputStream(
                    ServerConfigurations.getCredentialsEncryptionBuffer());
            InputStream infile = new ByteArrayInputStream(initialArray);

            DESEncryptor encrypt = new DESEncryptor(outfile, ServerConfigurations.getCredentialsEncryptionPassword());
            byte[] buff = new byte[2048];

            while (infile.available() > 0) {
                int r = infile.read(buff);
                encrypt.write(buff, 0, r);
            }
            encrypt.close(); // Close and write last data

            endArray = outfile.toByteArray();
            String encryptedHexString = StringShort.toHex(endArray);

            encrypt.close();
            infile.close();

            return encryptedHexString;

        } catch (Exception e) {
            logger.error("Credentials encryption error: " + e.getMessage());
            return null;
        }
    }

    private static String decrypt(String encryptedHex) {
        try {
            byte[] bytes = Hex.decodeHex(encryptedHex.toCharArray());

            InputStream infile = new ByteArrayInputStream(bytes);
            ByteArrayOutputStream outfile = new ByteArrayOutputStream(
                    ServerConfigurations.getCredentialsEncryptionBuffer());

            DESDecryptor decrypt = new DESDecryptor(infile, ServerConfigurations.getCredentialsEncryptionPassword());
            byte[] buff = new byte[500];

            int r = decrypt.read(buff, 0, 500);
            outfile.write(buff, 0, r);
            while (r > 0) {
                r = decrypt.read(buff, 0, 500);
                outfile.write(buff, 0, r);
            }
            decrypt.close();
            outfile.flush();
            outfile.close();
            byte[] decrypted = outfile.toByteArray();

            return new String(decrypted);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
