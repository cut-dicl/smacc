package edu.cut.smacc.configuration;

import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.policy.admission.AdmissionEXD;
import edu.cut.smacc.server.cache.policy.eviction.placement.DeleteOrDowngrade;
import edu.cut.smacc.server.cache.policy.eviction.trigger.ThresholdPercentageTrigger;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemEXD;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemFIFO;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemLFU;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemLIFE;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemLRFU;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemLRU;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemMRU;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import edu.cut.smacc.server.cache.policy.eviction.placement.EvictionPlacementPolicy;
import edu.cut.smacc.server.cache.policy.eviction.trigger.EvictionTriggerPolicy;
import edu.cut.smacc.server.cache.policy.selection.DiskSelectionLowerUsage;
import edu.cut.smacc.server.cache.policy.selection.DiskSelectionRoundRobin;
import edu.cut.smacc.server.cache.policy.selection.DiskSelectionPolicy;
import edu.cut.smacc.server.cache.policy.admission.AdmissionAlways;
import edu.cut.smacc.server.cache.policy.admission.AdmissionPolicy;
import edu.cut.smacc.server.statistics.output.StatisticsOutputClientBased;
import edu.cut.smacc.server.statistics.output.StatisticsOutputInvokePolicy;
import edu.cut.smacc.server.statistics.output.StatisticsOutputOperationCountBased;
import edu.cut.smacc.server.statistics.output.StatisticsOutputTimeBased;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Default configurations and loading of user configurations for smacc server
 *
 * @author Theodoros Danos
 * @author Michail Boronikolas
 */
public class ServerConfigurations extends BaseConfigurations {
    private static final Logger logger = LogManager.getLogger(ServerConfigurations.class);
    public static final String CACHE_DISK_VOLUME_STRING = "cache.disk.volume.";

    private static volatile boolean configsLoaded = false;

    //user configurable
    private static int S3_MAX_MULTIUPLOAD_CREATE_REQUEST_RETRIES;    //max retries to try to create multi-upload request
    private static int S3_MAX_PARALLEL_AUTOUPLOAD_LOCAL_LIMIT;
    private static int S3_MAX_PARALLEL_AUTOUPLOAD_GLOBAL_LIMIT;
    private static int S3_UPLOAD_CONNECTIONTIMEOUT;
    private static int S3_UPLOAD_SOCKETTIMEOUT;
    private static int S3_BYTE_BUFFER_SIZE;
    private static int S3_MAX_UPLOAD_RETRIES;
    private static int S3_MAX_UPLOAD_RETRY_DELAY;
    private static int S3_MIN_PARTSIZE_ALLOWED; // minimum part size the amazon allows (except for the last file)
    private static int S3_MULTI_UPLOAD_PART_SIZE;
    private static int ASYNCHRONOUS_UPLOADING_THREADPOOL_SIZE;
    private static int PARALLEL_UPLOAD_HANDLER_THREADPOOL_SIZE;
    private static int ASYNCHRONOUS_UPLOAD_BUFFER;
    private static int MEMORY_RECOVER_POOL_SIZE;
    private static String CACHE_SIGNATURE;
    private static int S3_OUT_OF_MEM_BLOCK_AND_UPLOAD_BUFFER_SIZE;
    private static int DOWNGRATION_HANDLER_THREAD_POOL_SIZE;
    private static int EVICTION_HANDLER_RUN_MS; // 5 every 5 seconds
    private static String CREDENTIALS_FILENAME;
    private static int CREDENTIALS_LOOKUP_TIME_MS;
    private static boolean DELETE_STATEFILE_ON_FAILURE;
    private static boolean OVERFLOW_MEMORY;    //	When memory is full, allow to write the last buffer to overflow the max capacity limit or not.
    private static int SERVER_PORT;
    private static int SERVER_QUEUE_SIZE;
    private static int SERVER_BUFFER_SIZE;
    private static int SERVER_READ_TIMEOUT_MS;
    private static HashMap<Integer, StoreSettings> SERVER_DISK_VOLUMES = null;
    private static StoreSettings SERVER_MEMORY_SETTINGS = null;
    private static int MEMORY_BYTE_BUFFER_SIZE;
    private static int CLIENT_HANDLING_THREAD_POOL_SIZE; // This also works as a queue - if the thread pool is full no
                                                         // other clients will get a response until other clients are
                                                         // finished
    private static int MANUAL_UPLOAD_CLIENT_RECONNECT_WAIT_MS;
    private static int MANUAL_UPLOAD_CLIENT_RECONNECT_MAX_RETRIES;
    private static int KEEP_ALIVE_TIME;

    private static boolean SNS_NOTIF_ACTIVATE;
    private static String SNS_AMAZON_ENDPOINT;
    private static String SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT;
    private static int SNS_LOCAL_PORT;
    private static String SNS_TOPIC_NAME;
    private static int SNS_NOTIF_POOL_SIZE;

    private static List<String> REQUEST_TIERS = new ArrayList<String>();
    private static boolean EVICTION_DELETE_DOWNGRADE; //delete OR downgrade

    private static boolean CACHE_RECOVERY_ACTIVATE;

    private static int SERVER_TO_S3_BUFFER_SIZE;

    /**
     * Keys, descriptions and default values for the configuration file
     */

    public static final String S3_MAX_MULTIUPLOAD_CREATE_REQUEST_RETRIES_KEY = "s3.upload.max.createRequests";
    public static final String S3_MAX_MULTIUPLOAD_CREATE_REQUEST_RETRIES_DESCRIPTION = "The maximum number of retries to create a multi-part upload request";
    public static final int S3_MAX_MULTIUPLOAD_CREATE_REQUEST_RETRIES_DEFAULT = 15;

    public static final String S3_MAX_PARALLEL_AUTOUPLOAD_LOCAL_LIMIT_KEY = "s3.upload.parallelUpload.local.max";
    public static final String S3_MAX_PARALLEL_AUTOUPLOAD_LOCAL_LIMIT_DESCRIPTION = "The maximum number of parallel uploads to S3 from the local disk";
    public static final int S3_MAX_PARALLEL_AUTOUPLOAD_LOCAL_LIMIT_DEFAULT = 0;

    public static final String S3_MAX_PARALLEL_AUTOUPLOAD_GLOBAL_LIMIT_KEY = "s3.upload.parallelUpload.global.max";
    public static final String S3_MAX_PARALLEL_AUTOUPLOAD_GLOBAL_LIMIT_DESCRIPTION = "The maximum number of parallel uploads to S3 from all disks";
    public static final int S3_MAX_PARALLEL_AUTOUPLOAD_GLOBAL_LIMIT_DEFAULT = 0;

    public static final String S3_UPLOAD_CONNECTIONTIMEOUT_KEY = "s3.upload.con.connectionTimeout.ms";
    public static final String S3_UPLOAD_CONNECTIONTIMEOUT_DESCRIPTION = "The connection timeout for S3 uploads";
    public static final int S3_UPLOAD_CONNECTIONTIMEOUT_DEFAULT = 3000;

    public static final String S3_UPLOAD_SOCKETTIMEOUT_KEY = "s3.upload.con.socketTimeout.ms";
    public static final String S3_UPLOAD_SOCKETTIMEOUT_DESCRIPTION = "The socket timeout for S3 uploads";
    public static final int S3_UPLOAD_SOCKETTIMEOUT_DEFAULT = 3000;

    public static final String S3_BYTE_BUFFER_SIZE_KEY = "s3.memory.byteBufferSize.byte";
    public static final String S3_BYTE_BUFFER_SIZE_DESCRIPTION = "The size of the byte buffer used for S3 uploads";
    public static final int S3_BYTE_BUFFER_SIZE_DEFAULT = 1024 * 1024;

    public static final String S3_MAX_UPLOAD_RETRIES_KEY = "s3.upload.max.upload.retry.times";
    public static final String S3_MAX_UPLOAD_RETRIES_DESCRIPTION = "The maximum number of retries to upload a file to S3";
    public static final int S3_MAX_UPLOAD_RETRIES_DEFAULT = 15;

    public static final String S3_MAX_UPLOAD_RETRY_DELAY_KEY = "s3.upload.max.upload.retry.delay.ms";
    public static final String S3_MAX_UPLOAD_RETRY_DELAY_DESCRIPTION = "The maximum delay between retries to upload a file to S3";
    public static final int S3_MAX_UPLOAD_RETRY_DELAY_DEFAULT = 30000;

    public static final String S3_MULTI_UPLOAD_PART_SIZE_KEY = "s3.upload.mutlipart.size.byte";
    public static final String S3_MULTI_UPLOAD_PART_SIZE_DESCRIPTION = "The size of the parts used for multi-part uploads to S3";
    public static final int S3_MULTI_UPLOAD_PART_SIZE_DEFAULT = 20 * 1024 * 1024;

    public static final String AMAZON_MIN_PARTSIZE_ALLOWED_KEY = "s3.upload.min.partsize.byte";
    public static final String AMAZON_MIN_PARTSIZE_ALLOWED_DESCRIPTION = "The minimum size of the parts used for multi-part uploads to S3";
    public static final int AMAZON_MIN_PARTSIZE_ALLOWED_DEFAULT = 5 * 1024 * 1024;

    public static final String ASYNCHRONOUS_UPLOADING_THREADPOOL_SIZE_KEY = "cache.parallel.asyncupload.threadpool.size";
    public static final String ASYNCHRONOUS_UPLOADING_THREADPOOL_SIZE_DESCRIPTION = "The size of the thread pool used for asynchronous uploads";
    public static final int ASYNCHRONOUS_UPLOADING_THREADPOOL_SIZE_DEFAULT = 1;

    public static final String ASYNCHRONOUS_UPLOAD_BUFFER_KEY = "cache.parallel.asyncupload.buffer.byte";
    public static final String ASYNCHRONOUS_UPLOAD_BUFFER_DESCRIPTION = "The size of the buffer used for asynchronous uploads";
    public static final int ASYNCHRONOUS_UPLOAD_BUFFER_DEFAULT = 1024 * 1024;

    public static final String PARALLEL_UPLOAD_HANDLER_THREADPOOL_SIZE_KEY = "cache.parallel.uploadhandler.threadpool.size";
    public static final String PARALLEL_UPLOAD_HANDLER_THREADPOOL_SIZE_DESCRIPTION = "The size of the thread pool used for parallel uploads";
    public static final int PARALLEL_UPLOAD_HANDLER_THREADPOOL_SIZE_DEFAULT = 20;

    public static final String MEMORY_RECOVER_POOL_SIZE_KEY = "cache.parallel.uploadrecovery.threadpool.size";
    public static final String MEMORY_RECOVER_POOL_SIZE_DESCRIPTION = "The size of the thread pool used for recovering memory";
    public static final int MEMORY_RECOVER_POOL_SIZE_DEFAULT = 4;

    public static final String CACHE_SIGNATURE_KEY = "cache.static.signature";
    public static final String CACHE_SIGNATURE_DESCRIPTION = "The signature of the cache";
    public static final String CACHE_SIGNATURE_DEFAULT = "SMACC_CACHE";

    public static final String S3_OUT_OF_MEM_BLOCK_AND_UPLOAD_BUFFER_SIZE_KEY = "cache.outOfMemory.block.upload.buffer.size";
    public static final String S3_OUT_OF_MEM_BLOCK_AND_UPLOAD_BUFFER_SIZE_DESCRIPTION = "The size of the buffer used for uploading blocks when the cache is out of memory";
    public static final int S3_OUT_OF_MEM_BLOCK_AND_UPLOAD_BUFFER_SIZE_DEFAULT = 100000;

    public static final String DOWNGRATION_HANDLER_THREAD_POOL_SIZE_KEY = "cache.outOfMemory.downgrade.threadpool.size";
    public static final String DOWNGRATION_HANDLER_THREAD_POOL_SIZE_DESCRIPTION = "The size of the thread pool used for downgrading the cache";
    public static final int DOWNGRATION_HANDLER_THREAD_POOL_SIZE_DEFAULT = 1;

    public static final String EVICTION_HANDLER_RUN_MS_KEY = "cache.outOfMemory.eviction.run.ms";
    public static final String EVICTION_HANDLER_RUN_MS_DESCRIPTION = "The time between eviction runs";
    public static final int EVICTION_HANDLER_RUN_MS_DEFAULT = 5000;

    public static final String OVERFLOW_MEMORY_KEY = "cache.outOfMemory.block.allow.overflow";
    public static final String OVERFLOW_MEMORY_DESCRIPTION = "Whether to allow the cache to overflow to disk";
    public static final boolean OVERFLOW_MEMORY_DEFAULT = false;

    public static final String CACHE_RECOVERY_ACTIVATE_KEY = "cache.recovery.activate";
    public static final String CACHE_RECOVERY_ACTIVATE_DESCRIPTION = "Whether or not to activate cache recovery at startup";
    public static final boolean CACHE_RECOVERY_ACTIVATE_DEFAULT = false;

    public static final String CREDENTIALS_FILENAME_KEY = "cache.recovery.backup.credentials.file";
    public static final String CREDENTIALS_FILENAME_DESCRIPTION = "The file containing the credentials for the backup S3 bucket";
    public static final String CREDENTIALS_FILENAME_DEFAULT = "cache/credentialsLogger.dat";

    public static final String CREDENTIALS_LOOKUP_TIME_MS_KEY = "cache.recovery.backup.credentials.lookup.ms";
    public static final String CREDENTIALS_LOOKUP_TIME_MS_DESCRIPTION = "The time between looking up the credentials for the backup S3 bucket";
    public static final int CREDENTIALS_LOOKUP_TIME_MS_DEFAULT = 60 * 1000;

    public static final String DELETE_STATEFILE_ON_FAILURE_KEY = "cache.recovery.backup.onFailure.delete.statefile";
    public static final String DELETE_STATEFILE_ON_FAILURE_DESCRIPTION = "Whether to delete the state file on failure";
    public static final boolean DELETE_STATEFILE_ON_FAILURE_DEFAULT = true;

    public static final String SERVER_PORT_KEY = "server.port";
    public static final String SERVER_PORT_DESCRIPTION = "The port to run the server on";
    public static final int SERVER_PORT_DEFAULT = 1111;

    public static final String SERVER_QUEUE_SIZE_KEY = "server.client.queue.size";
    public static final String SERVER_QUEUE_SIZE_DESCRIPTION = "The size of the queue for the server";
    public static final int SERVER_QUEUE_SIZE_DEFAULT = 200;

    public static final String SERVER_BUFFER_SIZE_KEY = "server.client.buffer.size";
    public static final String SERVER_BUFFER_SIZE_DESCRIPTION = "The size of the buffer for the server";
    public static final int SERVER_BUFFER_SIZE_DEFAULT = 2 * 1024 * 1024;

    public static final String SERVER_READ_TIMEOUT_MS_KEY = "server.client.read.timeout.ms";
    public static final String SERVER_READ_TIMEOUT_MS_DESCRIPTION = "The read timeout for the server";
    public static final int SERVER_READ_TIMEOUT_MS_DEFAULT = 30000;

    public static final String MEMORY_BYTE_BUFFER_SIZE_KEY = "cache.memory.byteBufferSize.byte";
    public static final String MEMORY_BYTE_BUFFER_SIZE_DESCRIPTION = "The size of the byte buffer used for memory";
    public static final int MEMORY_BYTE_BUFFER_SIZE_DEFAULT = 1024 * 1024;

    public static final String CLIENT_HANDLING_THREAD_POOL_KEY = "server.client.handle.threadpool.size";
    public static final String CLIENT_HANDLING_THREAD_POOL_DESCRIPTION = "The size of the thread pool used for handling clients";
    public static final int CLIENT_HANDLING_THREAD_POOL_DEFAULT = 20;

    public static final String MANUAL_UPLOAD_CLIENT_RECONNECT_WAIT_MS_KEY = "server.client.manualUpload.reconnect.wait.ms";
    public static final String MANUAL_UPLOAD_CLIENT_RECONNECT_WAIT_MS_DESCRIPTION = "The time to wait between reconnecting to the server";
    public static final int MANUAL_UPLOAD_CLIENT_RECONNECT_WAIT_MS_DEFAULT = 50;

    public static final String MANUAL_UPLOAD_CLIENT_RECONNECT_MAX_RETRIES_KEY = "server.client.manualUpload.reconnect.retries";
    public static final String MANUAL_UPLOAD_CLIENT_RECONNECT_MAX_RETRIES_DESCRIPTION = "The maximum number of times to reconnect to the server";
    public static final int MANUAL_UPLOAD_CLIENT_RECONNECT_MAX_RETRIES_DEFAULT = 1600;

    public static final String KEEP_ALIVE_TIME_KEY = "server.client.keepAlive.ms";
    public static final String KEEP_ALIVE_TIME_DESCRIPTION = "The time to wait before closing a connection";
    public static final int KEEP_ALIVE_TIME_DEFAULT = 300;

    public static final String SERVER_TO_S3_BUFFER_SIZE_KEY = "server.to.s3.buffer.size";
    public static final String SERVER_TO_S3_BUFFER_SIZE_DESCRIPTION = "The size of the buffer used for uploading a file to s3";
    public static final int SERVER_TO_S3_BUFFER_SIZE_DEFAULT = 1048576; // 1 MB

    // Cache settings
    public static final String CACHE_MEMORY_CAPACITY_KEY = "cache.memory.capacity";
    public static final String CACHE_MEMORY_CAPACITY_DESCRIPTION = "The capacity of the memory cache";
    public static final long CACHE_MEMORY_CAPACITY_DEFAULT = 2000000;

    public static final String CACHE_MEMORY_STATE_KEY = "cache.memory.state";
    public static final String CACHE_MEMORY_STATE_DESCRIPTION = "The directory to store the memory cache state";
    public static final String CACHE_MEMORY_STATE_DEFAULT = "cache/MemState/";

    public static final String CACHE_DISK_VOLUMES_SIZE_KEY = "cache.disk.volumes.size";
    public static final String CACHE_DISK_VOLUMES_SIZE_DESCRIPTION = "The number of disk volumes to use";
    public static final int CACHE_DISK_VOLUMES_SIZE_DEFAULT = 2;

    public static final String SNS_NOTIF_ACTIVATE_KEY = "sns.notification.activate";
    public static final String SNS_NOTIF_ACTIVATE_DESCRIPTION = "Whether or not to activate the SNS notification pool";
    public static final boolean SNS_NOTIF_ACTIVATE_DEFAULT = false;

    public static final String SNS_TOPIC_NAME_KEY = "sns.topic.name";
    public static final String SNS_TOPIC_NAME_DESCRIPTION = "The name of the SNS topic";
    public static final String SNS_TOPIC_NAME_DEFAULT = "SMACC_S3_EVENTS";

    public static final String SNS_NOTIF_POOL_SIZE_KEY = "sns.notification.pool.size";
    public static final String SNS_NOTIF_POOL_SIZE_DESCRIPTION = "The size of the SNS notification pool";
    public static final int SNS_NOTIF_POOL_SIZE_DEFAULT =  4;

    public static final String SNS_LOCAL_PORT_KEY = "sns.local.port";
    public static final String SNS_LOCAL_PORT_DESCRIPTION = "The local port to use for SNS";
    public static final int SNS_LOCAL_PORT_DEFAULT = 8000;

    public static final String SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT_KEY = "sns.notification.hostname.or.endpoint";
    public static final String SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT_DESCRIPTION = "The hostname or endpoint to use for SNS";
    public static final String SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT_DEFAULT = "127.0.0.1";

    public static final String SNS_AMAZON_ENDPOINT_KEY = "sns.amazon.endpoint";
    public static final String SNS_AMAZON_ENDPOINT_DESCRIPTION = "The endpoint to use for SNS";
    public static final String SNS_AMAZON_ENDPOINT_DEFAULT = "sns.amazonaws.com";

    // Cache admission and eviction policies

    public static final String ADMISSION_POLICY_CLASS_KEY = "admission.policy.class";
    public static final String ADMISSION_POLICY_CLASS_DESCRIPTION = "The class that implements the admission policy";
    public static final Class<? extends AdmissionPolicy> ADMISSION_POLICY_CLASS_DEFAULT = AdmissionAlways.class;
    public static final List<String> ADMISSION_POLICY_CLASS_ALTERNATIVES = List.of(
            AdmissionAlways.class.getName(),
            AdmissionEXD.class.getName());

    public static final String EVICTION_TRIGGER_POLICY_KEY = "eviction.trigger.policy";
    public static final String EVICTION_TRIGGER_POLICY_DESCRIPTION = "The class that implements the eviction trigger policy";
    public static final Class<? extends EvictionTriggerPolicy> EVICTION_TRIGGER_POLICY_DEFAULT = ThresholdPercentageTrigger.class;

    public static final String EVICTION_PLACEMENT_POLICY_KEY = "eviction.placement.policy";
    public static final String EVICTION_PLACEMENT_POLICY_DESCRIPTION = "The class that implements the eviction placement policy";
    public static final Class<? extends EvictionPlacementPolicy> EVICTION_PLACEMENT_POLICY_DEFAULT = DeleteOrDowngrade.class;

    public static final String EVICTION_ITEM_POLICY_KEY = "eviction.item.policy";
    public static final String EVICTION_ITEM_POLICY_DESCRIPTION = "The class that implements the eviction item policy";
    public static final Class<? extends EvictionItemPolicy> EVICTION_ITEM_POLICY_DEFAULT = EvictionItemLRU.class;
    public static final List<String> EVICTION_ITEM_POLICY_ALTERNATIVES = List.of(
            EvictionItemLRU.class.getName(),
            EvictionItemLFU.class.getName(),
            EvictionItemFIFO.class.getName(),
            EvictionItemMRU.class.getName(),
            EvictionItemLRFU.class.getName(),
            EvictionItemLIFE.class.getName(),
            EvictionItemEXD.class.getName());

    public static final String EVICTION_POLICY_WINDOW_BASED_AGING_HOURS_KEY = "eviction.policy.window.based.aging.hours";
    public static final String EVICTION_POLICY_WINDOW_BASED_AGING_HOURS_DESCRIPTION = "Window based aging parameter in hours (used with LIFE policy).";
    public static final int EVICTION_POLICY_WINDOW_BASED_AGING_HOURS_DEFAULT = 3;

    public static final String DISK_SELECTION_POLICY_KEY = "disk.selection.policy";
    public static final String DISK_SELECTION_POLICY_DESCRIPTION = "The class that implements the disk selection policy";
    public static final Class<? extends DiskSelectionPolicy> DISK_SELECTION_POLICY_DEFAULT = DiskSelectionRoundRobin.class;
    public static final List<String> DISK_SELECTION_POLICY_ALTERNATIVES = List.of(
            DiskSelectionRoundRobin.class.getName(),
            DiskSelectionLowerUsage.class.getName());

    public static final String CACHING_POLICY_WEIGHTED_BIAS_HOURS_KEY = "caching.policy.weighted.bias.hours";
    public static final String CACHING_POLICY_WEIGHTED_BIAS_HOURS_DESCRIPTION = "Weighted bias parameter in hours (used with EXD policy).";
    public static final int CACHING_POLICY_WEIGHTED_BIAS_HOURS_DEFAULT = 1;

    public static final String EVICTION_TRIGGER_POLICY_THRESHOLD_KEY = "eviction.trigger.policy.threshold";
    public static final String EVICTION_TRIGGER_POLICY_THRESHOLD_DESCRIPTION = "The threshold for the eviction trigger policy";
    public static final double EVICTION_TRIGGER_POLICY_THRESHOLD_DEFAULT = 85;

    public static final String STATISTICS_OUTPUT_INVOKE_POLICY_KEY = "statistics.output.invoke.policy";
    public static final String STATISTICS_OUTPUT_INVOKE_POLICY_DESCRIPTION = "The class that implements the statistics output invoke policy";
    public static final Class<? extends StatisticsOutputInvokePolicy> STATISTICS_OUTPUT_INVOKE_POLICY_DEFAULT = StatisticsOutputOperationCountBased.class;
    public static final List<String> STATISTICS_OUTPUT_INVOKE_POLICY_ALTERNATIVES = List.of(
            StatisticsOutputClientBased.class.getName(),
            StatisticsOutputOperationCountBased.class.getName(),
            StatisticsOutputTimeBased.class.getName());

    public static final String STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_KEY = "statistics.output.invoke.policy.metric";
    public static final String STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_DESCRIPTION = "The metric for the statistics output invoke policy";
    public static final int STATISTICS_OUTPUT_INVOKE_POLICY_METRIC_DEFAULT = 1;

    public static final String UPLOAD_LOCATION_KEY = "admission.location.tiers.on.put";
    public static final String UPLOAD_LOCATION_DESCRIPTION = "The placement location/s for uploads";
    public static final String UPLOAD_LOCATION_DEFAULT = "MEMORYDISK";

    public static final String REQUEST_LOCATION_KEY = "admission.location.tiers.on.get";
    public static final String REQUEST_LOCATION_DESCRIPTION = "The placement location/s for requests";
    public static final String REQUEST_LOCATION_DEFAULT = "MEMORYDISK";

    public static final String EVICTION_DELETE_DOWNGRADE_KEY = "eviction.placement.downgrade.on.evict";
    public static final String EVICTION_DELETE_DOWNGRADE_DESCRIPTION = "Whether the eviction policy should downgrade (or delete) the evicted item";
    public static final boolean EVICTION_DELETE_DOWNGRADE_DEFAULT = true;

    public static int getS3MaxCreateRequests() {
        return S3_MAX_MULTIUPLOAD_CREATE_REQUEST_RETRIES;
    }

    public static int getS3MaxParallelUploadLocal() {
        return S3_MAX_PARALLEL_AUTOUPLOAD_LOCAL_LIMIT;
    }

    public static int getS3MaxParallelUploadGlobal() {
        return S3_MAX_PARALLEL_AUTOUPLOAD_GLOBAL_LIMIT;
    }

    public static int getS3ConnectionTimeout() {
        return S3_UPLOAD_CONNECTIONTIMEOUT;
    }

    public static int getS3SocketnTimeout() {
        return S3_UPLOAD_SOCKETTIMEOUT;
    }

    public static int getS3ByteBufferSize() {
        return S3_BYTE_BUFFER_SIZE;
    }

    public static int getS3MaxUploadRetries() {
        return S3_MAX_UPLOAD_RETRIES;
    }

    public static int getS3MaxUploadRetryDelay() {
        return S3_MAX_UPLOAD_RETRY_DELAY;
    }

    public static int getS3MultiPartSize() {
        return S3_MULTI_UPLOAD_PART_SIZE;
    }

    public static int getAsyncThreadPoolSize() {
        return ASYNCHRONOUS_UPLOADING_THREADPOOL_SIZE;
    }

    public static int getParallelUploadHandlerThreadPoolSize() {
        return PARALLEL_UPLOAD_HANDLER_THREADPOOL_SIZE;
    }

    public static int getAsynchronousUploadBuffer() {
        return ASYNCHRONOUS_UPLOAD_BUFFER;
    }

    public static int getMemoryRecoverServicePoolSize() {
        return MEMORY_RECOVER_POOL_SIZE;
    }

    public static String getCacheSignature() {
        return CACHE_SIGNATURE;
    }

    public static int getS3BlockAndUploadBufferSize() {
        return S3_OUT_OF_MEM_BLOCK_AND_UPLOAD_BUFFER_SIZE;
    }

    public static int getDowngradeBufferSize() {
        return 200000;
    }/* Non-user configurable */

    public static int getDowngrationHanlderTheadPoolSize() {
        return DOWNGRATION_HANDLER_THREAD_POOL_SIZE;
    }

    public static int getEvictionHandlerRun() {
        return EVICTION_HANDLER_RUN_MS;
    }

    public static boolean getSNSNotificationActivate() {
        return SNS_NOTIF_ACTIVATE;
    }

    public static String getSNSAmazonHostname() {
        return SNS_AMAZON_ENDPOINT;
    }

    public static String getSNSHostnameOrEndpoint() {
        return SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT;
    }

    public static int getSNSLocalPort() {
        return SNS_LOCAL_PORT;
    }

    public static String getSNSTopicName() {
        return SNS_TOPIC_NAME;
    }

    public static int getNotificationProcessingPoolSize() {
        return SNS_NOTIF_POOL_SIZE;
    }

    public static int getDownloadNewFileBufferSize() {
        return 5000;
    }

    public static int getWaitPendingFilesTimeMs() {
        return 10000;
    }

    public static String getCredentialsLoggerFilename() {
        return CREDENTIALS_FILENAME;
    }

    public static int getCredentialsBackupLookupTimeMS() {
        return CREDENTIALS_LOOKUP_TIME_MS;
    }

    public static String getCredentialsEncryptionPassword() {
        return "DICL_DES";
    }//must have length of >= 8 characters

    public static int getCredentialsEncryptionBuffer() {
        return 4096;
    }

    public static boolean getCacheRecoveryActivate() {
        return CACHE_RECOVERY_ACTIVATE;
    }

    public static int recoveryBufferSize() {
        return 10000;
    }

    public static boolean deleteStateFileOnFailure() {
        return DELETE_STATEFILE_ON_FAILURE;
    }

    public static int getServerPort() {
        return SERVER_PORT;
    }

    public static int getServerQueueSize() {
        return SERVER_QUEUE_SIZE;
    }

    public static int getServerBufferSize() {
        return SERVER_BUFFER_SIZE;
    }

    public static int getServerReadTimeout() {
        return SERVER_READ_TIMEOUT_MS;
    }

    public static StoreSettings getServerMemorySettigs() {
        return SERVER_MEMORY_SETTINGS;
    }

    public static int getMemoryByteBufferSize() {
        return MEMORY_BYTE_BUFFER_SIZE;
    }

    public static int getClientHandlingThreadPoolSize() {
        return CLIENT_HANDLING_THREAD_POOL_SIZE;
    }

    public static int getManualUploadReconnectWaitMs() {
        return MANUAL_UPLOAD_CLIENT_RECONNECT_WAIT_MS;
    }

    public static int getManualUploadMaxRetries() {
        return MANUAL_UPLOAD_CLIENT_RECONNECT_MAX_RETRIES;
    }

    public static int getKeepAliveTime() {
        return KEEP_ALIVE_TIME;
    }

    public static HashMap<Integer, StoreSettings> getServerDiskVolumes() {
        return SERVER_DISK_VOLUMES;
    }

    public static boolean overflowMemory() {
        return OVERFLOW_MEMORY;
    }

    public static List<String> getRequestTiers() {
    	return REQUEST_TIERS;
    }

    
    public static boolean getEvictionDeleteOrDowngrade() {
    	return EVICTION_DELETE_DOWNGRADE;
    }

    public static int getServerToS3BufferSize() {
        return SERVER_TO_S3_BUFFER_SIZE;
    }

    public static void configsSet() {
        while (!configsLoaded)
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
    }

    public static void initialize(Configuration configuration) throws ConfigurationException {
        if (configsLoaded)
            return;

        BaseConfigurations.initialize(configuration);

        S3_MAX_MULTIUPLOAD_CREATE_REQUEST_RETRIES = configuration.getInt(S3_MAX_MULTIUPLOAD_CREATE_REQUEST_RETRIES_KEY,
                S3_MAX_MULTIUPLOAD_CREATE_REQUEST_RETRIES_DEFAULT);
        S3_MAX_PARALLEL_AUTOUPLOAD_LOCAL_LIMIT = configuration.getInt(S3_MAX_PARALLEL_AUTOUPLOAD_LOCAL_LIMIT_KEY,
                S3_MAX_PARALLEL_AUTOUPLOAD_LOCAL_LIMIT_DEFAULT);
        S3_MAX_PARALLEL_AUTOUPLOAD_GLOBAL_LIMIT = configuration.getInt(S3_MAX_PARALLEL_AUTOUPLOAD_GLOBAL_LIMIT_KEY,
                S3_MAX_PARALLEL_AUTOUPLOAD_GLOBAL_LIMIT_DEFAULT);
        S3_UPLOAD_CONNECTIONTIMEOUT = configuration.getInt(S3_UPLOAD_CONNECTIONTIMEOUT_KEY, S3_UPLOAD_CONNECTIONTIMEOUT_DEFAULT);
        S3_UPLOAD_SOCKETTIMEOUT = configuration.getInt(S3_UPLOAD_SOCKETTIMEOUT_KEY, S3_UPLOAD_SOCKETTIMEOUT_DEFAULT);
        S3_BYTE_BUFFER_SIZE = configuration.getInt(S3_BYTE_BUFFER_SIZE_KEY, S3_BYTE_BUFFER_SIZE_DEFAULT);
        S3_MAX_UPLOAD_RETRIES = configuration.getInt(S3_MAX_UPLOAD_RETRIES_KEY, S3_MAX_UPLOAD_RETRIES_DEFAULT);
        S3_MAX_UPLOAD_RETRY_DELAY = configuration.getInt(S3_MAX_UPLOAD_RETRY_DELAY_KEY, S3_MAX_UPLOAD_RETRY_DELAY_DEFAULT);
        S3_MULTI_UPLOAD_PART_SIZE = configuration.getInt(S3_MULTI_UPLOAD_PART_SIZE_KEY, S3_MULTI_UPLOAD_PART_SIZE_DEFAULT);
        S3_MIN_PARTSIZE_ALLOWED = configuration.getInt(AMAZON_MIN_PARTSIZE_ALLOWED_KEY,
                AMAZON_MIN_PARTSIZE_ALLOWED_DEFAULT);
        if (S3_MULTI_UPLOAD_PART_SIZE >= S3_MIN_PARTSIZE_ALLOWED)
            S3_MULTI_UPLOAD_PART_SIZE = S3_MIN_PARTSIZE_ALLOWED;

        ASYNCHRONOUS_UPLOADING_THREADPOOL_SIZE = configuration.getInt(ASYNCHRONOUS_UPLOADING_THREADPOOL_SIZE_KEY,
                ASYNCHRONOUS_UPLOADING_THREADPOOL_SIZE_DEFAULT);
        ASYNCHRONOUS_UPLOAD_BUFFER = configuration.getInt(ASYNCHRONOUS_UPLOAD_BUFFER_KEY, ASYNCHRONOUS_UPLOAD_BUFFER_DEFAULT);
        PARALLEL_UPLOAD_HANDLER_THREADPOOL_SIZE = configuration.getInt(PARALLEL_UPLOAD_HANDLER_THREADPOOL_SIZE_KEY,
                PARALLEL_UPLOAD_HANDLER_THREADPOOL_SIZE_DEFAULT);
        MEMORY_RECOVER_POOL_SIZE = configuration.getInt(MEMORY_RECOVER_POOL_SIZE_KEY, MEMORY_RECOVER_POOL_SIZE_DEFAULT);
        CACHE_SIGNATURE = configuration.getString(CACHE_SIGNATURE_KEY, CACHE_SIGNATURE_DEFAULT);
        S3_OUT_OF_MEM_BLOCK_AND_UPLOAD_BUFFER_SIZE = configuration.getInt(S3_OUT_OF_MEM_BLOCK_AND_UPLOAD_BUFFER_SIZE_KEY,
                S3_OUT_OF_MEM_BLOCK_AND_UPLOAD_BUFFER_SIZE_DEFAULT);
        DOWNGRATION_HANDLER_THREAD_POOL_SIZE = configuration.getInt(DOWNGRATION_HANDLER_THREAD_POOL_SIZE_KEY,
                DOWNGRATION_HANDLER_THREAD_POOL_SIZE_DEFAULT);
        EVICTION_HANDLER_RUN_MS = configuration.getInt(EVICTION_HANDLER_RUN_MS_KEY, EVICTION_HANDLER_RUN_MS_DEFAULT);
        OVERFLOW_MEMORY = configuration.getBoolean(OVERFLOW_MEMORY_KEY, OVERFLOW_MEMORY_DEFAULT);
        CREDENTIALS_FILENAME = configuration.getString(CREDENTIALS_FILENAME_KEY, CREDENTIALS_FILENAME_DEFAULT);
        CREDENTIALS_LOOKUP_TIME_MS = configuration.getInt(CREDENTIALS_LOOKUP_TIME_MS_KEY, CREDENTIALS_LOOKUP_TIME_MS_DEFAULT);
        DELETE_STATEFILE_ON_FAILURE = configuration.getBoolean(DELETE_STATEFILE_ON_FAILURE_KEY, DELETE_STATEFILE_ON_FAILURE_DEFAULT);

        SERVER_PORT = configuration.getInt(SERVER_PORT_KEY, SERVER_PORT_DEFAULT);
        SERVER_QUEUE_SIZE = configuration.getInt(SERVER_QUEUE_SIZE_KEY, SERVER_QUEUE_SIZE_DEFAULT);
        SERVER_BUFFER_SIZE = configuration.getInt(SERVER_BUFFER_SIZE_KEY, SERVER_BUFFER_SIZE_DEFAULT);
        SERVER_READ_TIMEOUT_MS = configuration.getInt(SERVER_READ_TIMEOUT_MS_KEY, SERVER_READ_TIMEOUT_MS_DEFAULT);

        MEMORY_BYTE_BUFFER_SIZE = configuration.getInt(MEMORY_BYTE_BUFFER_SIZE_KEY, MEMORY_BYTE_BUFFER_SIZE_DEFAULT);
        CLIENT_HANDLING_THREAD_POOL_SIZE = configuration.getInt(CLIENT_HANDLING_THREAD_POOL_KEY,
                CLIENT_HANDLING_THREAD_POOL_DEFAULT);
        MANUAL_UPLOAD_CLIENT_RECONNECT_WAIT_MS = configuration.getInt(MANUAL_UPLOAD_CLIENT_RECONNECT_WAIT_MS_KEY,
                MANUAL_UPLOAD_CLIENT_RECONNECT_WAIT_MS_DEFAULT);
        MANUAL_UPLOAD_CLIENT_RECONNECT_MAX_RETRIES = configuration.getInt(MANUAL_UPLOAD_CLIENT_RECONNECT_MAX_RETRIES_KEY,
                MANUAL_UPLOAD_CLIENT_RECONNECT_MAX_RETRIES_DEFAULT);
        KEEP_ALIVE_TIME = configuration.getInt(KEEP_ALIVE_TIME_KEY, KEEP_ALIVE_TIME_DEFAULT);

        CACHE_RECOVERY_ACTIVATE = configuration.getBoolean(CACHE_RECOVERY_ACTIVATE_KEY,
                CACHE_RECOVERY_ACTIVATE_DEFAULT);

        SERVER_TO_S3_BUFFER_SIZE = configuration.getInt(SERVER_TO_S3_BUFFER_SIZE_KEY, SERVER_TO_S3_BUFFER_SIZE_DEFAULT);

        initializeCacheSettings(configuration);

        SNS_NOTIF_ACTIVATE = configuration.getBoolean(SNS_NOTIF_ACTIVATE_KEY, SNS_NOTIF_ACTIVATE_DEFAULT);
        SNS_TOPIC_NAME = configuration.getString(SNS_TOPIC_NAME_KEY, SNS_TOPIC_NAME_DEFAULT);
        SNS_NOTIF_POOL_SIZE = configuration.getInt(SNS_NOTIF_POOL_SIZE_KEY, SNS_NOTIF_POOL_SIZE_DEFAULT);
        SNS_LOCAL_PORT = configuration.getInt(SNS_LOCAL_PORT_KEY, SNS_LOCAL_PORT_DEFAULT);
        if (!configuration.containsKey(SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT_KEY)) {
            try {
                SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT = InetAddress.getLocalHost().getHostAddress();
            } catch (Exception e) {
                logger.error("Could not translate server host address (s3.sns.hostname)");
                System.exit(1);
            }
        }
        SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT = configuration.getString(SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT_KEY,
                SNS_NOTIFICATION_HOSTNAME_OR_ENDPOINT_DEFAULT);
        if (!configuration.containsKey(SNS_AMAZON_ENDPOINT_KEY)) {
            SNS_AMAZON_ENDPOINT = "sns." + getDefaultRegion() + ".amazonaws.com";
        } else {
            SNS_AMAZON_ENDPOINT = configuration.getString(SNS_AMAZON_ENDPOINT_KEY);
        }

        /* Verify that client will have shorter read timeout (because having equal or greater will cause sending packets to nowhere without the chance to re-send them */
        if (getServerReadTimeout() - 2000 < ClientConfigurations.getClientReadTimeout()) {
            logger.error("Server read timeout should be at least 2 seconds greater than client's read timeout");
            System.exit(1);
        }

        if (S3_MAX_UPLOAD_RETRY_DELAY < SERVER_READ_TIMEOUT_MS) {
            logger.error("Server read timeout should be less than upload retry delay time");
            System.exit(1);
        }


        /* Config */
        EVICTION_DELETE_DOWNGRADE = configuration.getBoolean(EVICTION_DELETE_DOWNGRADE_KEY, EVICTION_DELETE_DOWNGRADE_DEFAULT);

        configsLoaded = true;
    }

    private static void initializeCacheSettings(Configuration configuration) {
        // Cache settings
        long memoryCapacity = configuration.getLong(CACHE_MEMORY_CAPACITY_KEY, CACHE_MEMORY_CAPACITY_DEFAULT);
        String memoryState = configuration.getString(CACHE_MEMORY_STATE_KEY, CACHE_MEMORY_STATE_DEFAULT);
        File memoryStateFile = new File(memoryState);
        if (memoryCapacity > 0 && memoryStateFile.isDirectory()) {
            SERVER_MEMORY_SETTINGS = new StoreSettings(null, memoryState, new UsageStats(memoryCapacity));
        } else {
            logger.info("No memory settings found [capacity, state folder]");
        }

        // Disk settings
        int diskVolumesSize = configuration.getInt(CACHE_DISK_VOLUMES_SIZE_KEY, CACHE_DISK_VOLUMES_SIZE_DEFAULT);
        if (diskVolumesSize <= 0) {
            logger.info("Disk volumes set to 0");
            // System.exit(1);
        }
        SERVER_DISK_VOLUMES = new HashMap<>(diskVolumesSize);
        for (int i = 0; i < diskVolumesSize; ++i) {
            String volStr = CACHE_DISK_VOLUME_STRING + i;
            if (!configuration.containsKey(volStr)) {
                logger.error("Volume " + i + " not found");
                return;
            }
            // Format: /path/to/disk/volume,/path/to/state/folder,1000000000
            String[] diskVolumes = configuration.getString(volStr).split(",");
            if (!(diskVolumes.length == 3)) {
                logger.error("Disk Volume Format: mainFolder, stateFolder, capacity");
                return;
            }
            for (int j = 0; j < diskVolumes.length; ++j) {
                diskVolumes[j] = diskVolumes[j].trim();
            }
            File diskVolume = new File(diskVolumes[0]);
            File diskState = new File(diskVolumes[1]);
            long diskCapacity = Long.parseLong(diskVolumes[2]);
            if (!diskVolume.isDirectory()) {
                logger.error("Disk Volume Directory Error :" + diskVolumes[0]);
                return;
            }
            if (!diskState.isDirectory()) {
                logger.error("Disk State Directory Error :" + diskVolumes[1]);
                return;
            }
            if (diskCapacity <= 0) {
                logger.error("Disk Capacity Error :" + diskVolumes[2]);
                return;
            }
            SERVER_DISK_VOLUMES.put(i, new StoreSettings(diskVolumes[0], diskVolumes[1],
                    new UsageStats(diskCapacity)));
        }
    }

}
