package edu.cut.smacc.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.cut.smacc.server.cloud.CloudInfo;

/**
 * Common Configurations between Client and Server are held in this class
 */
public abstract class BaseConfigurations {

    private static final Logger logger = LogManager.getLogger(BaseConfigurations.class);

    private static String BACKEND_CLOUD_STORAGE;

    private static String S3_TIER_MASTER_ACCESS_KEY;
    private static String S3_TIER_MASTER_SECRET_KEY;
    private static String S3_DEFAULT_REGION;
    private static String S3_DEFAULT_BUCKET;
    private static String S3_AMAZON_ENDPOINT;

    public static final String BACKEND_CLOUD_STORAGE_KEY = "backend.cloud.storage.system";
    public static final String BACKEND_CLOUD_STORAGE_DESCRIPTION = "The backend cloud storage system to use: S3 or MinIO";
    public static final String BACKEND_CLOUD_STORAGE_DEFAULT = "S3";

    public static final String S3_TIER_MASTER_ACCESS_KEY_KEY = "s3.credentials.access.key";
    public static final String S3_TIER_MASTER_ACCESS_KEY_DESCRIPTION = "The access key for the S3 bucket";
    public static final String S3_TIER_MASTER_ACCESS_KEY_DEFAULT = null;

    public static final String S3_TIER_MASTER_SECRET_KEY_KEY = "s3.credentials.secret.key";
    public static final String S3_TIER_MASTER_SECRET_KEY_DESCRIPTION = "The secret key for the S3 bucket";
    public static final String S3_TIER_MASTER_SECRET_KEY_DEFAULT = null;

    public static final String S3_DEFAULT_REGION_KEY = "s3.default.region";
    public static final String S3_DEFAULT_REGION_DESCRIPTION = "The default region to use for S3";
    public static final String S3_DEFAULT_REGION_DEFAULT = null;

    public static final String S3_DEFAULT_BUCKET_KEY = "s3.default.bucket";
    public static final String S3_DEFAULT_BUCKET_DESCRIPTION = "The default bucket to use for S3";
    public static final String S3_DEFAULT_BUCKET_DEFAULT = null;

    public static final String S3_AMAZON_ENDPOINT_KEY = "s3.amazon.endpoint";
    public static final String S3_AMAZON_ENDPOINT_DESCRIPTION = "The endpoint to use for S3";
    public static final String S3_AMAZON_ENDPOINT_DEFAULT = "s3.amazonaws.com";

    public static String getBackendCloudStorage() {
        return BACKEND_CLOUD_STORAGE;
    }

    public static String getMasterAccessKey() {
        return S3_TIER_MASTER_ACCESS_KEY;
    }

    public static String getMasterSecretKey() {
        return S3_TIER_MASTER_SECRET_KEY;
    }

    public static String getDefaultRegion() {
        return S3_DEFAULT_REGION;
    }

    public static String getDefaultBucket() {
        return S3_DEFAULT_BUCKET;
    }

    public static String getS3AmazonEndpoint() {
        return S3_AMAZON_ENDPOINT;
    }

    public static CloudInfo getDefaultCloudInfo() {
        return new CloudInfo(S3_AMAZON_ENDPOINT, S3_DEFAULT_REGION,
                S3_TIER_MASTER_ACCESS_KEY, S3_TIER_MASTER_SECRET_KEY);
    }

    public static void initialize(Configuration configuration) {
        BACKEND_CLOUD_STORAGE = configuration.getString(BACKEND_CLOUD_STORAGE_KEY, BACKEND_CLOUD_STORAGE_DEFAULT);
        S3_TIER_MASTER_ACCESS_KEY = configuration.getString(S3_TIER_MASTER_ACCESS_KEY_KEY, S3_TIER_MASTER_ACCESS_KEY_DEFAULT);
        S3_TIER_MASTER_SECRET_KEY = configuration.getString(S3_TIER_MASTER_SECRET_KEY_KEY, S3_TIER_MASTER_SECRET_KEY_DEFAULT);
        S3_DEFAULT_REGION = configuration.getString(S3_DEFAULT_REGION_KEY, S3_DEFAULT_REGION_DEFAULT);
        S3_DEFAULT_BUCKET = configuration.getString(S3_DEFAULT_BUCKET_KEY, S3_DEFAULT_BUCKET_DEFAULT);
        if (configuration.containsKey(S3_AMAZON_ENDPOINT_KEY)) {
            S3_AMAZON_ENDPOINT = configuration.getString(S3_AMAZON_ENDPOINT_KEY);
        } else {
            S3_AMAZON_ENDPOINT = "s3." + S3_DEFAULT_REGION + ".amazonaws.com";
        }

        // Something is missing, exit
        if (BACKEND_CLOUD_STORAGE == null
                || S3_TIER_MASTER_ACCESS_KEY == null
                || S3_TIER_MASTER_SECRET_KEY == null
                || S3_DEFAULT_BUCKET == null
                || S3_DEFAULT_REGION == null
                || S3_AMAZON_ENDPOINT == null) {
            logger.error("Missing Cloud Store required information");
            System.exit(-1);
        }
    }

}
