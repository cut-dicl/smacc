package edu.cut.smacc.server.cache;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.CacheManager;
import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.s3.S3File;

import java.io.IOException;

/**
 * A helper class that deals with creating and deleting cache files.
 */
public class CacheFileHelper {

    public static CacheFile createTierCacheFile(CacheManager cacheManager, CloudFile s3IS) throws IOException {
        CacheFile cFile = cacheManager.create(s3IS.getBucket(), s3IS.getKey());
        cFile.setActualSize(s3IS.getLength());
        cFile.setLastModified(s3IS.getLastModified());
        return cFile;
    }

    public static CacheFile createS3CacheFile(CloudFile s3IS) {
        CacheFile s3file = new S3File(s3IS.getBucket(), s3IS.getKey());
        s3file.setActualSize(s3IS.getLength());
        s3file.setLastModified(s3IS.getLastModified());
        return s3file;
    }

    public static CacheFile createS3CacheFile(CacheFile cacheFile) {
        CacheFile s3file = new S3File(cacheFile.getBucket(), cacheFile.getKey());
        s3file.setActualSize(cacheFile.getActualSize());
        s3file.setLastModified(cacheFile.getLastModified());
        return s3file;
    }

    public static boolean handleDelete(String bucket, String key, CacheManager cMgr) {
        /*
         * CacheFile file = cMgr.getFile(bucket, key);
         * if (file == null) {
         * return false;
         * }
         */
        return cMgr.delete(bucket, key);
    }

}
