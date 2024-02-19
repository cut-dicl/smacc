package edu.cut.smacc.client;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.intelligenttiering.IntelligentTieringConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.model.ownership.OwnershipControls;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;
import edu.cut.smacc.client.request.*;
import edu.cut.smacc.configuration.ClientConfigurations;
import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ConfigurationException;
import edu.cut.smacc.server.cache.common.BlockRange;
import edu.cut.smacc.server.cache.common.SMACCObject;
import edu.cut.smacc.server.cache.common.StateType;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.configuration.ServerConfigurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This smacc client is used as an alternative class of AmazonS3.
 * Using this class the client connects with smacc server
 *
 * @author Theodoros Danos
 */
public class SMACCClient implements AmazonS3 {
    private static final Logger logger = LogManager.getLogger(SMACCClient.class);

    private static final String DIRECTORY_DELIMITER = "/";

    private AmazonS3 s3Client;

    private BasicAWSCredentials clientCredentials;
    private String region;
    private String endPoint;
    private boolean default_async;

    private static String CONFIGURATION_PATH = "conf/client.config.properties";


    public SMACCClient() {
        String config = CONFIGURATION_PATH;
        File file = new File(config);
        if (!file.exists()) {
            logger.fatal("Configuration file not found");
            throw new SdkClientException("Configuration file not found");
        }
        Configuration conf = new Configuration(config);
        init(conf);
    }

    public SMACCClient(Configuration conf) {
        init(conf);
    }

    public static void setConfigurationPath(String path) {
    	CONFIGURATION_PATH = path;
    }

    private void init(Configuration conf) {
    	try {
            ClientConfigurations.initialize(conf);
        } catch (ConfigurationException e) {
            logger.fatal(e.getMessage(), e);
        }

        this.clientCredentials = new BasicAWSCredentials(ClientConfigurations.getMasterAccessKey(),
                ClientConfigurations.getMasterSecretKey());

        this.region = ClientConfigurations.getDefaultRegion();
        this.endPoint = ClientConfigurations.getS3AmazonEndpoint();
        this.default_async = ClientConfigurations.getClientModeAsync();

        ClientConfiguration s3ClientConfigs = new ClientConfiguration();
        s3ClientConfigs.setConnectionTimeout(ServerConfigurations.getS3ConnectionTimeout());
        s3ClientConfigs.setSocketTimeout(ServerConfigurations.getS3SocketnTimeout());

        EndpointConfiguration endpointConfiguration = new EndpointConfiguration(endPoint, region);
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(clientCredentials))
                .withEndpointConfiguration(endpointConfiguration)
                .withClientConfiguration(s3ClientConfigs).build();

        SmaccClientRequestFactory.init(clientCredentials, endPoint, region);
    }

    public void setRegion(com.amazonaws.regions.Region arg0) throws IllegalArgumentException {
        setRegion(arg0.getName());
    }

    private void setRegion(String region) {
        this.region = region;
        setClientConfiguration();
    }

    /* Set SMACC's Client Configurations */
    private void setClientConfiguration() {
        ClientConfiguration s3ClientConfigs = new ClientConfiguration();
        s3ClientConfigs.setConnectionTimeout(ServerConfigurations.getS3ConnectionTimeout());
        s3ClientConfigs.setSocketTimeout(ServerConfigurations.getS3SocketnTimeout());

        if (s3Client != null) s3Client.shutdown();
        EndpointConfiguration endpointConfiguration = new EndpointConfiguration(endPoint, region);
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(clientCredentials))
                .withEndpointConfiguration(endpointConfiguration)
                .withClientConfiguration(s3ClientConfigs).build();
    }

    public void setEndpoint(String endPoint) {
        this.endPoint = endPoint;
        setClientConfiguration();
    }

    /* PUT OBJECT */
    private PutObjectResult putObjectInput(String bucket, String key, boolean async, InputStream in, Long length) throws SdkClientException {
        if (length != null && length == 0) length = null;
        SmaccClientRequest putRequest = SmaccClientRequestFactory.createPutRequest(bucket, key, async, length);
        try {
            putRequest.initiate();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] buffer;
        if (length != null && length.longValue() > 0 && length.longValue() < ClientConfigurations.getClientBufferSize())
            buffer = new byte[(int) length.longValue()];
        else
            buffer = new byte[ClientConfigurations.getClientBufferSize()];

        PutObjectResult result = new PutObjectResult();
        OutputStream out = SmaccClientRequestAdapter.adaptToOutputStream(putRequest);
        try {
            while (in.available() > 0) {
                int r = in.read(buffer);
                out.write(buffer, 0, r);
            }

            result.setETag(SmaccClientRequestAdapter.getETagFromOutputStream(out));
            in.close();
            out.close();
        } catch (IOException e) {
            throw new SdkClientException(e);
        }
        return result;
    }

    public List<PutObjectResult> putDirectoryObject(String bucket, String keyDir, String destinationDir) throws SdkClientException {
        List<PutObjectResult> results = new ArrayList<>();
        File dir = new File(keyDir);
        File[] files = dir.listFiles();
        if (files == null) {
            throw new SdkClientException("Directory is empty");
        }
        for (File file : files) {
            if (file.isDirectory()) {
                results.addAll(putDirectoryObject(bucket, keyDir + DIRECTORY_DELIMITER +
                        file.getName(), destinationDir));
            } else {
                String s3FilePath = destinationDir + DIRECTORY_DELIMITER + file.getPath();
                s3FilePath = s3FilePath.replace('\\', '/');
                results.add(putObject(bucket, s3FilePath, file));
            }
        }
        return results;
    }

    public PutObjectResult putObject(String bucket, String key, File file) throws SdkClientException {
        InputStream in;
        try {
            in = new FileInputStream(file);
        } catch (FileNotFoundException exc) {
            throw new SdkClientException(exc);
        }

        return putObjectInput(bucket, key, default_async, in, file.length());
    }

    public PutObjectResult putObject(String bucket, String key, InputStream input, ObjectMetadata meta) {
        if (meta == null) {
            return putObject(bucket, key, input);
        }
        return putObjectInput(bucket, key, default_async, input, meta.getContentLength());
    }

    public PutObjectResult putObject(String bucket, String key, InputStream input) {
        return putObjectInput(bucket, key, default_async, input, null);
    }

    public PutObjectResult putObject(String bucket, String key, InputStream input, long length) {
        return putObjectInput(bucket, key, default_async, input, length);
    }

    public void putObject(String bucket, File fileToPut, String destinationPathOnS3) {
        this.putObject(bucket, destinationPathOnS3, fileToPut);
    }

    public PutObjectResult putObject(PutObjectRequest request) {
        InputStream in;
        Long length = null;
        if (request.getInputStream() != null) {
            in = request.getInputStream();
            try {
                length = (long) in.available();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                in = new FileInputStream(request.getFile());
                length = request.getFile().length();
            } catch (FileNotFoundException exc) {
                throw new SdkClientException(exc);
            }
        }

        return putObjectInput(request.getBucketName(), request.getKey(), default_async, in, length);
    }

    public PutObjectResult putAsynchronousObject(String bucket, String key, InputStream input) throws SdkClientException {
        return putObjectInput(bucket, key, true, input, null);
    }

    public PutObjectResult putAsynchronousObject(String bucket, String key, InputStream input, long length) throws SdkClientException {
        return putObjectInput(bucket, key, true, input, length);
    }

    /* LIST */
    public List<SMACCObject> cacheList(String bucket) throws SdkClientException {
        return cacheList(bucket, null);
    }

    public List<SMACCObject> cacheList(String bucket, String prefix) throws SdkClientException {
        SmaccClientRequest request = SmaccClientRequestFactory.createListCacheRequest(bucket, prefix);
        try {
            request.initiate();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<SMACCObject> smaccObjects = ((SmaccClientListCacheRequest) request).getSmaccObjects();
        try {
            request.close();
        } catch (IOException e) { // ignore
        }
        return smaccObjects;
    }

    /* GET */
    @Override
    public S3Object getObject(String s, String s1) throws SdkClientException {
        return getObject(s, s1, null);
    }

    public S3Object getObject(String bucket, String key, String destinationFileKey) throws SdkClientException {
        return getObject(bucket, key, destinationFileKey, null);
    }

    void getDirectoryObject(String bucket, String sourceFileKey, String destinationFileKey) throws SdkClientException {
        List<S3ObjectSummary> dirObjects = listDirectoryObjects(bucket, sourceFileKey);
        if (dirObjects.isEmpty()) {
            throw new SdkClientException("File/Directory not found");
        }
        if (dirObjects.size() > 1 && Files.isRegularFile(Paths.get(destinationFileKey))) {
            throw new SdkClientException("Destination file is not a directory");
        }
        if (dirObjects.size() > 1) {
            new File(destinationFileKey).mkdirs();
        }

        for (S3ObjectSummary dirObject : dirObjects) {
            if (isDirectoryObject(bucket, dirObject.getKey())) {
                getDirectoryObject(bucket, dirObject.getKey(), destinationFileKey);
            } else {
                getObject(bucket, dirObject.getKey(), destinationFileKey);
            }
        }
    }

    public S3Object getObject(GetObjectRequest request) throws SdkClientException {
        long[] lrange = request.getRange();
        BlockRange range = null;
        if (lrange != null) {
            if (lrange.length == 2)
                range = new BlockRange(lrange[0], lrange[1]);
        }
        return getObject(request.getBucketName(), request.getKey(), null, range);
    }

    private S3Object getObject(String bucket, String sourceFileKey, String destinationFileKey,
                               BlockRange range) throws SdkClientException {
        /* Not all metadata exist on cache server - request them using s3 */
        ObjectMetadata meta = new ObjectMetadata();

        /* Construct S3Object */
        InputStream is;
        S3Object object = new S3Object();
        object.setBucketName(bucket);
        object.setKey(sourceFileKey);
        SmaccClientRequest getRequest = SmaccClientRequestFactory.createGetRequest(bucket, sourceFileKey, range);
        try {
            getRequest.initiate();
            is = SmaccClientRequestAdapter.adaptToInputStream(getRequest);
            object.setObjectContent(is);
        } catch (IOException e) {
            throw new AmazonServiceException(e.getMessage());
        }
        object.setObjectMetadata(meta);
        if (destinationFileKey == null) {
            // Don't save the data, just return the object
            return object;
        }

        // Create a file to put the InputStream bytes inside
        try {
            writeInputStreamToFile(is, sourceFileKey, destinationFileKey);
        } catch (IOException e) {
            throw new AmazonServiceException(e.getMessage());
        }

        try {
            getRequest.close();
        } catch (IOException e) { // ignore
        }

        return object;
    }

    private static void writeInputStreamToFile(InputStream inputStream, String sourceFile, String destinationFile)
            throws IOException {
        Path sourcePath = Paths.get(sourceFile);
        Path destinationPath = Paths.get(destinationFile);
        if (Files.isDirectory(destinationPath)) {
            destinationPath = destinationPath.resolve(sourcePath);
        }

        if (destinationPath.getParent() != null && !Files.exists(destinationPath.getParent())) {
            Files.createDirectories(destinationPath.getParent());
        }

        try (OutputStream outputStream = Files.newOutputStream(destinationPath)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* DELETE */
    public void deleteObject(DeleteObjectRequest request) throws SdkClientException {
        deleteObject2(request.getBucketName(), request.getKey());
    }

    public void deleteObject(String bucket, String key) throws SdkClientException {
        deleteObject2(bucket, key);
    }

    public boolean deleteObject2(String bucket, String key) throws SdkClientException {
            if (isDirectoryObject(bucket, key)) {
                List<S3ObjectSummary> directoryObjectKeyList = listDirectoryObjects(bucket, key);
                int directorySize = directoryObjectKeyList.size();
                for (S3ObjectSummary objSummary : directoryObjectKeyList) {
                    deleteObject(bucket, objSummary.getKey());
                    if (--directorySize == 0) {
                        return true;
                    }
                }
                return false;
            }
            SmaccClientRequest request = SmaccClientRequestFactory.createDeleteRequest(bucket, key);
            try {
                // Try to delete from SMACC cache and S3
                return request.initiate();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    request.close();
                } catch (IOException e) { // ignore
                }
            }
    }

    public boolean deleteCacheObject(String bucket, String key) throws SdkClientException {
        SmaccClientRequest request = SmaccClientRequestFactory.createDeleteCacheRequest(bucket, key);
        try {
            return request.initiate();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                request.close();
            } catch (IOException e) { // ignore
            }
        }
    }

    boolean isDirectoryObject(String bucket, String prefix) {
        if (!prefix.endsWith(DIRECTORY_DELIMITER)) {
            return false;
        }
        return !listDirectoryObjects(bucket, prefix).isEmpty();
    }

    public List<S3ObjectSummary> listDirectoryObjects(String bucket, String directoryPrefix) {
        ListObjectsRequest listRequest = new ListObjectsRequest()
                .withBucketName(bucket);
        if (directoryPrefix != null) {
            listRequest = listRequest
                    .withPrefix(directoryPrefix);
        }

        ObjectListing result = this.listObjects(listRequest);
        List<S3ObjectSummary> objects = new ArrayList<>(result.getObjectSummaries());
        while (result.isTruncated()) {
            result = this.listNextBatchOfObjects(result);
            objects.addAll(result.getObjectSummaries());
        }
        return objects;
    }

    /* STATISTICS REQUEST */
    public void statsRequest() {
        SmaccClientRequest request = SmaccClientRequestFactory.createStatsRequest();
        try {
            request.initiate();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                request.close();
            } catch (IOException e) { // ignore
            }
        }
    }

    public void resetStatsRequest() {
        SmaccClientRequest request = SmaccClientRequestFactory.createResetStatsRequest();
        try {
            request.initiate();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                request.close();
            } catch (IOException e) { // ignore
            }
        }
    }

    /* SHUTDOWN REQUEST */
    void shutdownRequest() {
        SmaccClientRequest request = SmaccClientRequestFactory.createShutdownRequest();
        try {
            request.initiate();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                request.close();
            } catch (IOException e) { // ignore
            }
        }
    }

    /* CLEAR CACHE REQUEST */
    public void clearCache() {
        SmaccClientRequest request = SmaccClientRequestFactory.createClearCacheRequest();
        try {
            request.initiate();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /* FILE STATUS REQUEST */
    public SMACCObject fileStatusRequest(String bucket, String key) {
        SmaccClientRequest request = SmaccClientRequestFactory.createFileStatusRequest(bucket, key);
        SMACCObject smaccObject = null;
        try {
            if (request.initiate()) {
                smaccObject = ((SmaccClientFileStatusRequest) request).getSmaccObject();
            } else {
                ObjectMetadata meta = getObjectMetadata(bucket, key);
                smaccObject = new SMACCObject(key, bucket, meta.getContentLength(), 0l, false, null,
                        StoreOptionType.S3_ONLY,
                        meta.getLastModified().getTime(), StateType.COMPLETE, null);
            }
        } catch (AmazonS3Exception s3e) {
            if (s3e.getStatusCode() != 404)
                s3e.printStackTrace(); // ignore file not found
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                request.close();
            } catch (IOException e) {
                // ignore
            }
        }

        return smaccObject;
    }

    /* Shutdown */
    public void shutdown() {
        s3Client.shutdown();
    }

    /* Not Supported Ops */
    public UploadPartResult uploadPart(UploadPartRequest arg0) throws SdkClientException {
        throw new SdkClientException("Operataion uploadPart Not Supported");
    }

    /* Native Ops */
    public void abortMultipartUpload(AbortMultipartUploadRequest arg0) throws SdkClientException {
        s3Client.abortMultipartUpload(arg0);
    }

    public void changeObjectStorageClass(String arg0, String arg1, StorageClass arg2) throws SdkClientException {
        CopyObjectRequest copyReq = new CopyObjectRequest(arg0, arg1, arg0, arg1);
        copyReq.setStorageClass(arg2);
        s3Client.copyObject(copyReq);
    }

    public CompleteMultipartUploadResult completeMultipartUpload(
            CompleteMultipartUploadRequest arg0) throws SdkClientException {
        return s3Client.completeMultipartUpload(arg0);
    }

    public CopyObjectResult copyObject(CopyObjectRequest arg0) throws SdkClientException {
        return s3Client.copyObject(arg0);
    }

    public CopyObjectResult copyObject(String arg0, String arg1, String arg2, String arg3) throws SdkClientException {
        return s3Client.copyObject(arg0, arg1, arg2, arg3);
    }

    public CopyPartResult copyPart(CopyPartRequest arg0) throws SdkClientException {
        return s3Client.copyPart(arg0);
    }

    public Bucket createBucket(CreateBucketRequest arg0) throws SdkClientException {
        return s3Client.createBucket(arg0);
    }

    public Bucket createBucket(String arg0) throws SdkClientException {
        return s3Client.createBucket(arg0);
    }

    public Bucket createBucket(String arg0, Region arg1) throws SdkClientException {
        return s3Client.createBucket(new CreateBucketRequest(arg0, arg1));
    }

    public Bucket createBucket(String arg0, String arg1) throws SdkClientException {
        return s3Client.createBucket(new CreateBucketRequest(arg0, arg1));
    }

    public void deleteBucket(DeleteBucketRequest arg0) throws SdkClientException {
        s3Client.deleteBucket(arg0);
    }

    public void deleteBucket(String arg0) throws SdkClientException {
        s3Client.deleteBucket(arg0);
    }

    public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(
            DeleteBucketAnalyticsConfigurationRequest arg0) throws SdkClientException {
        return s3Client.deleteBucketAnalyticsConfiguration(arg0);
    }

    public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(
            String arg0, String arg1) throws SdkClientException {
        return s3Client.deleteBucketAnalyticsConfiguration(arg0, arg1);
    }

    public void deleteBucketCrossOriginConfiguration(String arg0) {
        s3Client.deleteBucketCrossOriginConfiguration(arg0);
    }

    public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest arg0) {
        s3Client.deleteBucketCrossOriginConfiguration(arg0);
    }

    public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(
            DeleteBucketInventoryConfigurationRequest arg0) throws SdkClientException {
        return s3Client.deleteBucketInventoryConfiguration(arg0);
    }

    public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(
            String arg0, String arg1) throws SdkClientException {
        return s3Client.deleteBucketInventoryConfiguration(arg0, arg1);
    }

    public void deleteBucketLifecycleConfiguration(String arg0) {
        s3Client.deleteBucketLifecycleConfiguration(arg0);
    }

    public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest arg0) {
        s3Client.deleteBucketLifecycleConfiguration(arg0);
    }

    public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(
            DeleteBucketMetricsConfigurationRequest arg0) throws SdkClientException {
        return s3Client.deleteBucketMetricsConfiguration(arg0);
    }

    public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(
            String arg0, String arg1) throws SdkClientException {
        return s3Client.deleteBucketMetricsConfiguration(arg0, arg1);
    }

    public void deleteBucketPolicy(String arg0) throws SdkClientException {
        s3Client.deleteBucketPolicy(arg0);
    }

    public void deleteBucketPolicy(DeleteBucketPolicyRequest arg0) throws SdkClientException {
        s3Client.deleteBucketPolicy(arg0);
    }

    public void deleteBucketReplicationConfiguration(String arg0) throws SdkClientException {
        s3Client.deleteBucketReplicationConfiguration(arg0);
    }

    public void deleteBucketReplicationConfiguration(DeleteBucketReplicationConfigurationRequest arg0)
            throws SdkClientException {
        s3Client.deleteBucketReplicationConfiguration(arg0);
    }

    public void deleteBucketTaggingConfiguration(String arg0) {
        s3Client.deleteBucketTaggingConfiguration(arg0);
    }

    public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest arg0) {
        s3Client.deleteBucketTaggingConfiguration(arg0);
    }

    public void deleteBucketWebsiteConfiguration(String arg0) throws SdkClientException {
        s3Client.deleteBucketWebsiteConfiguration(arg0);
    }

    public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest arg0) throws SdkClientException {
        s3Client.deleteBucketWebsiteConfiguration(arg0);
    }

    public DeleteObjectTaggingResult deleteObjectTagging(DeleteObjectTaggingRequest arg0) {
        return s3Client.deleteObjectTagging(arg0);
    }

    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest arg0) throws SdkClientException {
        return s3Client.deleteObjects(arg0);
    }

    public void deleteVersion(DeleteVersionRequest arg0) throws SdkClientException {
        s3Client.deleteVersion(arg0);
    }

    public void deleteVersion(String arg0, String arg1, String arg2) throws SdkClientException {
        s3Client.deleteVersion(arg0, arg1, arg2);
    }

    public void disableRequesterPays(String arg0) throws SdkClientException {
        s3Client.disableRequesterPays(arg0);
    }

    public boolean doesBucketExist(String arg0) throws SdkClientException {
        return s3Client.doesBucketExistV2(arg0);
    }

    public boolean doesBucketExistV2(String arg0) throws SdkClientException {
        return s3Client.doesBucketExistV2(arg0);
    }

    public boolean doesObjectExist(String arg0, String arg1) throws SdkClientException {
        return s3Client.doesObjectExist(arg0, arg1);
    }

    public void enableRequesterPays(String arg0) throws SdkClientException {
        s3Client.enableRequesterPays(arg0);
    }

    public URL generatePresignedUrl(GeneratePresignedUrlRequest arg0) throws SdkClientException {
        return s3Client.generatePresignedUrl(arg0);
    }

    public URL generatePresignedUrl(String arg0, String arg1, Date arg2) throws SdkClientException {
        return s3Client.generatePresignedUrl(arg0, arg1, arg2);
    }

    public URL generatePresignedUrl(String arg0, String arg1, Date arg2, HttpMethod arg3) throws SdkClientException {
        return s3Client.generatePresignedUrl(arg0, arg1, arg2, arg3);
    }

    public BucketAccelerateConfiguration getBucketAccelerateConfiguration(String arg0) throws SdkClientException {
        return s3Client.getBucketAccelerateConfiguration(arg0);
    }

    public BucketAccelerateConfiguration getBucketAccelerateConfiguration(
            GetBucketAccelerateConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketAccelerateConfiguration(arg0);
    }

    public AccessControlList getBucketAcl(String arg0) throws SdkClientException {
        return s3Client.getBucketAcl(arg0);
    }

    public AccessControlList getBucketAcl(GetBucketAclRequest arg0) throws SdkClientException {
        return s3Client.getBucketAcl(arg0);
    }

    public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(
            GetBucketAnalyticsConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketAnalyticsConfiguration(arg0);
    }

    public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(
            String arg0, String arg1) throws SdkClientException {
        return s3Client.getBucketAnalyticsConfiguration(arg0, arg1);
    }

    public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String arg0) {
        return s3Client.getBucketCrossOriginConfiguration(arg0);
    }

    public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(GetBucketCrossOriginConfigurationRequest arg0) {
        return s3Client.getBucketCrossOriginConfiguration(arg0);
    }

    public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(
            GetBucketInventoryConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketInventoryConfiguration(arg0);
    }

    public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(
            String arg0, String arg1) throws SdkClientException {
        return s3Client.getBucketInventoryConfiguration(arg0, arg1);
    }

    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String arg0) {
        return s3Client.getBucketLifecycleConfiguration(arg0);
    }

    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest arg0) {
        return s3Client.getBucketLifecycleConfiguration(arg0);
    }

    public String getBucketLocation(String arg0) throws SdkClientException {
        return s3Client.getBucketLocation(arg0);
    }

    public String getBucketLocation(GetBucketLocationRequest arg0) throws SdkClientException {
        return s3Client.getBucketLocation(arg0);
    }

    public BucketLoggingConfiguration getBucketLoggingConfiguration(String arg0) throws SdkClientException {
        return s3Client.getBucketLoggingConfiguration(arg0);
    }

    public BucketLoggingConfiguration getBucketLoggingConfiguration(
            GetBucketLoggingConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketLoggingConfiguration(arg0);
    }

    public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(
            GetBucketMetricsConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketMetricsConfiguration(arg0);
    }

    public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(
            String arg0, String arg1) throws SdkClientException {
        return s3Client.getBucketMetricsConfiguration(arg0, arg1);
    }

    public BucketNotificationConfiguration getBucketNotificationConfiguration(
            String arg0) throws SdkClientException {
        return s3Client.getBucketNotificationConfiguration(arg0);
    }

    public BucketNotificationConfiguration getBucketNotificationConfiguration(
            GetBucketNotificationConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketNotificationConfiguration(arg0);
    }

    public BucketPolicy getBucketPolicy(String arg0) throws SdkClientException {
        return s3Client.getBucketPolicy(arg0);
    }

    public BucketPolicy getBucketPolicy(GetBucketPolicyRequest arg0) throws SdkClientException {
        return s3Client.getBucketPolicy(arg0);
    }

    public BucketReplicationConfiguration getBucketReplicationConfiguration(String arg0) throws SdkClientException {
        return s3Client.getBucketReplicationConfiguration(arg0);
    }

    public BucketReplicationConfiguration getBucketReplicationConfiguration(
            GetBucketReplicationConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketReplicationConfiguration(arg0);
    }

    public BucketTaggingConfiguration getBucketTaggingConfiguration(String arg0) {
        return s3Client.getBucketTaggingConfiguration(arg0);
    }

    public BucketTaggingConfiguration getBucketTaggingConfiguration(GetBucketTaggingConfigurationRequest arg0) {
        return s3Client.getBucketTaggingConfiguration(arg0);
    }

    public BucketVersioningConfiguration getBucketVersioningConfiguration(String arg0) throws SdkClientException {
        return s3Client.getBucketVersioningConfiguration(arg0);
    }

    public BucketVersioningConfiguration getBucketVersioningConfiguration(
            GetBucketVersioningConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketVersioningConfiguration(arg0);
    }

    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String arg0) throws SdkClientException {
        return s3Client.getBucketWebsiteConfiguration(arg0);
    }

    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(
            GetBucketWebsiteConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketWebsiteConfiguration(arg0);
    }

    public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest arg0) {
        return s3Client.getCachedResponseMetadata(arg0);
    }

    public ObjectMetadata getObject(GetObjectRequest arg0, File arg1) throws SdkClientException {
        return s3Client.getObject(arg0, arg1);
    }

    public AccessControlList getObjectAcl(GetObjectAclRequest arg0) throws SdkClientException {
        return s3Client.getObjectAcl(arg0);
    }

    public AccessControlList getObjectAcl(String arg0, String arg1) throws SdkClientException {
        return s3Client.getObjectAcl(arg0, arg1);
    }

    public AccessControlList getObjectAcl(String arg0, String arg1, String arg2) throws SdkClientException {
        return s3Client.getObjectAcl(arg0, arg1, arg2);
    }

    public String getObjectAsString(String arg0, String arg1) throws SdkClientException {
        return s3Client.getObjectAsString(arg0, arg1);
    }

    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest arg0) throws SdkClientException {
        return s3Client.getObjectMetadata(arg0);
    }

    public ObjectMetadata getObjectMetadata(String arg0, String arg1) throws SdkClientException {
        return s3Client.getObjectMetadata(arg0, arg1);
    }

    public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest arg0) {
        return s3Client.getObjectTagging(arg0);
    }

    public Region getRegion() {
        return s3Client.getRegion();
    }

    public String getRegionName() {
        return s3Client.getRegionName();
    }

    public Owner getS3AccountOwner() throws SdkClientException {
        return s3Client.getS3AccountOwner();
    }

    public Owner getS3AccountOwner(GetS3AccountOwnerRequest arg0) throws SdkClientException {
        return s3Client.getS3AccountOwner(arg0);
    }

    public URL getUrl(String arg0, String arg1) {
        return s3Client.getUrl(arg0, arg1);
    }

    public HeadBucketResult headBucket(HeadBucketRequest arg0) throws SdkClientException {
        return s3Client.headBucket(arg0);
    }

    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest arg0) throws SdkClientException {
        return s3Client.initiateMultipartUpload(arg0);
    }

    public boolean isRequesterPaysEnabled(String arg0) throws SdkClientException {
        return s3Client.isRequesterPaysEnabled(arg0);
    }

    public ListBucketAnalyticsConfigurationsResult listBucketAnalyticsConfigurations(
            ListBucketAnalyticsConfigurationsRequest arg0) throws SdkClientException {
        return s3Client.listBucketAnalyticsConfigurations(arg0);
    }

    public ListBucketInventoryConfigurationsResult listBucketInventoryConfigurations(
            ListBucketInventoryConfigurationsRequest arg0) throws SdkClientException {
        return s3Client.listBucketInventoryConfigurations(arg0);
    }

    @Override
    public DeleteBucketEncryptionResult deleteBucketEncryption(String bucketName) throws SdkClientException {
        return s3Client.deleteBucketEncryption(bucketName);
    }

    @Override
    public DeleteBucketEncryptionResult deleteBucketEncryption(DeleteBucketEncryptionRequest request) throws SdkClientException {
        return s3Client.deleteBucketEncryption(request);
    }

    @Override
    public GetBucketEncryptionResult getBucketEncryption(String bucketName) throws SdkClientException {
        return s3Client.getBucketEncryption(bucketName);
    }

    @Override
    public GetBucketEncryptionResult getBucketEncryption(GetBucketEncryptionRequest request) throws SdkClientException {
        return s3Client.getBucketEncryption(request);
    }

    @Override
    public SetBucketEncryptionResult setBucketEncryption(SetBucketEncryptionRequest setBucketEncryptionRequest) throws SdkClientException {
        return s3Client.setBucketEncryption(setBucketEncryptionRequest);
    }

    @Override
    public SetPublicAccessBlockResult setPublicAccessBlock(SetPublicAccessBlockRequest request) {
        return s3Client.setPublicAccessBlock(request);
    }

    @Override
    public GetPublicAccessBlockResult getPublicAccessBlock(GetPublicAccessBlockRequest request) {
        return s3Client.getPublicAccessBlock(request);
    }

    @Override
    public DeletePublicAccessBlockResult deletePublicAccessBlock(DeletePublicAccessBlockRequest request) {
        return s3Client.deletePublicAccessBlock(request);
    }

    @Override
    public GetBucketPolicyStatusResult getBucketPolicyStatus(GetBucketPolicyStatusRequest request) {
        return getBucketPolicyStatus(request);
    }

    @Override
    public SelectObjectContentResult selectObjectContent(SelectObjectContentRequest selectRequest) throws SdkClientException {
        return s3Client.selectObjectContent(selectRequest);
    }

    @Override
    public SetObjectLegalHoldResult setObjectLegalHold(SetObjectLegalHoldRequest setObjectLegalHoldRequest) {
        return s3Client.setObjectLegalHold(setObjectLegalHoldRequest);
    }

    @Override
    public GetObjectLegalHoldResult getObjectLegalHold(GetObjectLegalHoldRequest getObjectLegalHoldRequest) {
        return s3Client.getObjectLegalHold(getObjectLegalHoldRequest);
    }

    @Override
    public SetObjectLockConfigurationResult setObjectLockConfiguration(SetObjectLockConfigurationRequest setObjectLockConfigurationRequest) {
        return s3Client.setObjectLockConfiguration(setObjectLockConfigurationRequest);
    }

    @Override
    public GetObjectLockConfigurationResult getObjectLockConfiguration(GetObjectLockConfigurationRequest getObjectLockConfigurationRequest) {
        return s3Client.getObjectLockConfiguration(getObjectLockConfigurationRequest);
    }

    @Override
    public SetObjectRetentionResult setObjectRetention(SetObjectRetentionRequest setObjectRetentionRequest) {
        return s3Client.setObjectRetention(setObjectRetentionRequest);
    }

    @Override
    public GetObjectRetentionResult getObjectRetention(GetObjectRetentionRequest getObjectRetentionRequest) {
        return s3Client.getObjectRetention(getObjectRetentionRequest);
    }

    @Override
    public PresignedUrlDownloadResult download(PresignedUrlDownloadRequest presignedUrlDownloadRequest) {
        return s3Client.download(presignedUrlDownloadRequest);
    }

    @Override
    public void download(PresignedUrlDownloadRequest presignedUrlDownloadRequest, File destinationFile) {
        s3Client.download(presignedUrlDownloadRequest, destinationFile);
    }

    @Override
    public PresignedUrlUploadResult upload(PresignedUrlUploadRequest presignedUrlUploadRequest) {
        return upload(presignedUrlUploadRequest);
    }

    public ListBucketMetricsConfigurationsResult listBucketMetricsConfigurations(
            ListBucketMetricsConfigurationsRequest arg0) throws SdkClientException {
        return s3Client.listBucketMetricsConfigurations(arg0);
    }

    public List<Bucket> listBuckets() throws SdkClientException {
        return s3Client.listBuckets();
    }

    public List<Bucket> listBuckets(ListBucketsRequest arg0) throws SdkClientException {
        return s3Client.listBuckets();
    }

    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest arg0) throws SdkClientException {
        return s3Client.listMultipartUploads(arg0);
    }

    public ObjectListing listNextBatchOfObjects(ObjectListing arg0) throws SdkClientException {
        return s3Client.listNextBatchOfObjects(arg0);
    }

    public ObjectListing listNextBatchOfObjects(ListNextBatchOfObjectsRequest arg0) throws SdkClientException {
        return s3Client.listNextBatchOfObjects(arg0);
    }

    public VersionListing listNextBatchOfVersions(VersionListing arg0) throws SdkClientException {
        return s3Client.listNextBatchOfVersions(arg0);
    }

    public VersionListing listNextBatchOfVersions(ListNextBatchOfVersionsRequest arg0) throws SdkClientException {
        return s3Client.listNextBatchOfVersions(arg0);
    }

    public ObjectListing listObjects(String arg0) throws SdkClientException {
        return this.listObjects(arg0, null);
    }

    public ObjectListing listObjects(ListObjectsRequest arg0) throws SdkClientException {
        return s3Client.listObjects(arg0);
    }

    public ObjectListing listObjects(String arg0, String arg1) throws SdkClientException {
        return (arg1 != null) ?
                s3Client.listObjects(arg0, arg1) :
                s3Client.listObjects(arg0);
    }

    public ListObjectsV2Result listObjectsV2(String arg0) throws SdkClientException {
        return s3Client.listObjectsV2(arg0);
    }

    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request arg0) throws SdkClientException {
        return s3Client.listObjectsV2(arg0);
    }

    public ListObjectsV2Result listObjectsV2(String arg0, String arg1) throws SdkClientException {
        return s3Client.listObjectsV2(arg0, arg1);
    }

    public PartListing listParts(ListPartsRequest arg0) throws SdkClientException {
        return s3Client.listParts(arg0);
    }

    public VersionListing listVersions(ListVersionsRequest arg0) throws SdkClientException {
        return s3Client.listVersions(arg0);
    }

    public VersionListing listVersions(String arg0, String arg1) throws SdkClientException {
        return s3Client.listVersions(arg0, arg1);
    }

    public VersionListing listVersions(String arg0, String arg1, String arg2, String arg3, String arg4, Integer arg5)
            throws SdkClientException {
        return s3Client.listVersions(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    public PutObjectResult putObject(String arg0, String arg1, String arg2) throws SdkClientException {
        return s3Client.putObject(arg0, arg1, arg2);
    }

    public void restoreObject(RestoreObjectRequest arg0) throws AmazonServiceException {
        s3Client.restoreObjectV2(arg0);
    }

    @Override
    public RestoreObjectResult restoreObjectV2(RestoreObjectRequest request) throws AmazonServiceException {
        return s3Client.restoreObjectV2(request);
    }

    public void restoreObject(String arg0, String arg1, int arg2) throws AmazonServiceException {
        s3Client.restoreObjectV2(new RestoreObjectRequest(arg0, arg1, arg2));
    }

    public void setBucketAccelerateConfiguration(SetBucketAccelerateConfigurationRequest arg0) throws SdkClientException {
        s3Client.setBucketAccelerateConfiguration(arg0);
    }

    public void setBucketAccelerateConfiguration(String arg0, BucketAccelerateConfiguration arg1) throws SdkClientException {
        s3Client.setBucketAccelerateConfiguration(arg0, arg1);
    }

    public void setBucketAcl(SetBucketAclRequest arg0) throws SdkClientException {
        s3Client.setBucketAcl(arg0);
    }

    public void setBucketAcl(String arg0, AccessControlList arg1) throws SdkClientException {
        s3Client.setBucketAcl(arg0, arg1);
    }

    public void setBucketAcl(String arg0, CannedAccessControlList arg1) throws SdkClientException {
        s3Client.setBucketAcl(arg0, arg1);
    }

    public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(
            SetBucketAnalyticsConfigurationRequest arg0) throws SdkClientException {
        return s3Client.setBucketAnalyticsConfiguration(arg0);
    }

    public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(
            String arg0, AnalyticsConfiguration arg1) throws SdkClientException {
        return s3Client.setBucketAnalyticsConfiguration(arg0, arg1);
    }

    public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest arg0) {
        s3Client.setBucketCrossOriginConfiguration(arg0);
    }

    public void setBucketCrossOriginConfiguration(String arg0, BucketCrossOriginConfiguration arg1) {
        s3Client.setBucketCrossOriginConfiguration(arg0, arg1);
    }

    public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(
            SetBucketInventoryConfigurationRequest arg0) throws SdkClientException {
        return s3Client.setBucketInventoryConfiguration(arg0);
    }

    public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(
            String arg0, InventoryConfiguration arg1) throws SdkClientException {
        return s3Client.setBucketInventoryConfiguration(arg0, arg1);
    }

    public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest arg0) {
        s3Client.setBucketLifecycleConfiguration(arg0);
    }

    public void setBucketLifecycleConfiguration(String arg0, BucketLifecycleConfiguration arg1) {
        s3Client.setBucketLifecycleConfiguration(arg0, arg1);
    }

    public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest arg0) throws SdkClientException {
        s3Client.setBucketLoggingConfiguration(arg0);
    }

    public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(
            SetBucketMetricsConfigurationRequest arg0) throws SdkClientException {
        return s3Client.setBucketMetricsConfiguration(arg0);
    }

    public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(
            String arg0, MetricsConfiguration arg1) throws SdkClientException {
        return s3Client.setBucketMetricsConfiguration(arg0, arg1);
    }

    public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest arg0) throws SdkClientException {
        s3Client.setBucketNotificationConfiguration(arg0);
    }

    public void setBucketNotificationConfiguration(String arg0, BucketNotificationConfiguration arg1) throws SdkClientException {
        s3Client.setBucketNotificationConfiguration(arg0, arg1);
    }

    public void setBucketPolicy(SetBucketPolicyRequest arg0) throws SdkClientException {
        s3Client.setBucketPolicy(arg0);
    }

    public void setBucketPolicy(String arg0, String arg1) throws SdkClientException {
        s3Client.setBucketPolicy(arg0, arg1);
    }

    public void setBucketReplicationConfiguration(SetBucketReplicationConfigurationRequest arg0) throws SdkClientException {
        s3Client.setBucketReplicationConfiguration(arg0);
    }

    public void setBucketReplicationConfiguration(String arg0, BucketReplicationConfiguration arg1) throws SdkClientException {
        s3Client.setBucketReplicationConfiguration(arg0, arg1);
    }

    public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest arg0) {
        s3Client.setBucketTaggingConfiguration(arg0);
    }

    public void setBucketTaggingConfiguration(String arg0, BucketTaggingConfiguration arg1) {
        s3Client.setBucketTaggingConfiguration(arg0, arg1);
    }

    public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest arg0) throws SdkClientException {
        s3Client.setBucketVersioningConfiguration(arg0);
    }

    public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest arg0) throws SdkClientException {
        s3Client.setBucketWebsiteConfiguration(arg0);
    }

    public void setBucketWebsiteConfiguration(String arg0, BucketWebsiteConfiguration arg1) throws SdkClientException {
        s3Client.setBucketWebsiteConfiguration(arg0, arg1);
    }

    public void setObjectAcl(SetObjectAclRequest arg0) throws SdkClientException {
        s3Client.setObjectAcl(arg0);
    }

    public void setObjectAcl(String arg0, String arg1, AccessControlList arg2) throws SdkClientException {
        s3Client.setObjectAcl(arg0, arg1, arg2);
    }

    public void setObjectAcl(String arg0, String arg1, CannedAccessControlList arg2) throws SdkClientException {
        s3Client.setObjectAcl(arg0, arg1, arg2);
    }

    public void setObjectAcl(String arg0, String arg1, String arg2, AccessControlList arg3) throws SdkClientException {
        s3Client.setObjectAcl(arg0, arg1, arg2, arg3);
    }

    public void setObjectAcl(String arg0, String arg1, String arg2, CannedAccessControlList arg3) throws SdkClientException {
        s3Client.setObjectAcl(arg0, arg1, arg2, arg3);
    }

    public void setObjectRedirectLocation(String arg0, String arg1, String arg2) throws SdkClientException {
        s3Client.copyObject(new CopyObjectRequest(arg0, arg1, arg0, arg1).withRedirectLocation(arg2));
    }

    public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest arg0) {
        return s3Client.setObjectTagging(arg0);
    }

    public void setS3ClientOptions(S3ClientOptions arg0) {
        s3Client.setS3ClientOptions(arg0);
    }

    public AmazonS3Waiters waiters() {
        return s3Client.waiters();
    }

    @Override
    public DeleteBucketIntelligentTieringConfigurationResult deleteBucketIntelligentTieringConfiguration(
            DeleteBucketIntelligentTieringConfigurationRequest arg0) throws SdkClientException {
        return s3Client.deleteBucketIntelligentTieringConfiguration(arg0);
    }

    @Override
    public DeleteBucketIntelligentTieringConfigurationResult deleteBucketIntelligentTieringConfiguration(String arg0,
            String arg1) throws SdkClientException {
        return s3Client.deleteBucketIntelligentTieringConfiguration(arg0, arg1);
    }

    @Override
    public DeleteBucketOwnershipControlsResult deleteBucketOwnershipControls(DeleteBucketOwnershipControlsRequest arg0)
            throws SdkClientException {
        return s3Client.deleteBucketOwnershipControls(arg0);
    }

    @Override
    public GetBucketIntelligentTieringConfigurationResult getBucketIntelligentTieringConfiguration(
            GetBucketIntelligentTieringConfigurationRequest arg0) throws SdkClientException {
        return s3Client.getBucketIntelligentTieringConfiguration(arg0);
    }

    @Override
    public GetBucketIntelligentTieringConfigurationResult getBucketIntelligentTieringConfiguration(String arg0,
            String arg1) throws SdkClientException {
        return s3Client.getBucketIntelligentTieringConfiguration(arg0, arg1);
    }

    @Override
    public GetBucketOwnershipControlsResult getBucketOwnershipControls(GetBucketOwnershipControlsRequest arg0)
            throws SdkClientException {
        return s3Client.getBucketOwnershipControls(arg0);
    }

    @Override
    public ListBucketIntelligentTieringConfigurationsResult listBucketIntelligentTieringConfigurations(
            ListBucketIntelligentTieringConfigurationsRequest arg0) throws SdkClientException {
        return s3Client.listBucketIntelligentTieringConfigurations(arg0);
    }

    @Override
    public SetBucketIntelligentTieringConfigurationResult setBucketIntelligentTieringConfiguration(
            SetBucketIntelligentTieringConfigurationRequest arg0) throws SdkClientException {
        return s3Client.setBucketIntelligentTieringConfiguration(arg0);
    }

    @Override
    public SetBucketIntelligentTieringConfigurationResult setBucketIntelligentTieringConfiguration(String arg0,
            IntelligentTieringConfiguration arg1) throws SdkClientException {
        return s3Client.setBucketIntelligentTieringConfiguration(arg0, arg1);
    }

    @Override
    public SetBucketOwnershipControlsResult setBucketOwnershipControls(SetBucketOwnershipControlsRequest arg0)
            throws SdkClientException {
        return s3Client.setBucketOwnershipControls(arg0);
    }

    @Override
    public SetBucketOwnershipControlsResult setBucketOwnershipControls(String arg0, OwnershipControls arg1)
            throws SdkClientException {
        return s3Client.setBucketOwnershipControls(arg0, arg1);
    }

    @Override
    public void setRequestPaymentConfiguration(SetRequestPaymentConfigurationRequest arg0) {
        s3Client.setRequestPaymentConfiguration(arg0);
    }

    @Override
    public WriteGetObjectResponseResult writeGetObjectResponse(WriteGetObjectResponseRequest arg0) {
        return s3Client.writeGetObjectResponse(arg0);
    }
}
