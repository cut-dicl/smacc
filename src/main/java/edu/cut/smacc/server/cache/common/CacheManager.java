package edu.cut.smacc.server.cache.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * CacheManager interfaces defines the methods that cache managers should implement.
 */
public interface CacheManager {

    /**
     * Create a new file in the cache, with the given size
     * @param bucket bucket name
     * @param key file key
     * @param actualSize file size
     * @return the cache file
     * @throws IOException if there's a problem creating the file
     */
    CacheFile create(String bucket, String key, long actualSize) throws IOException;

    /**
     * Create a new file in the cache, without specifying the size
     * @param bucket bucket name
     * @param key file key
     * @return the cache file
     * @throws IOException if there's a problem creating the file
     */
    CacheFile create(String bucket, String key) throws IOException;

    /**
     * Get a file from the cache
     * @param bucket the bucket name
     * @param key the file key
     * @return the cache file
     */
    CacheFile getFile(String bucket, String key);

    /**
     * Check if a file exists in the cache
     * @param bucket bucket name
     * @param key file key
     * @return true if the file exists, false otherwise
     */
    boolean containsObject(String bucket, String key);

    /**
     * Read a whole file from the cache
     * @param bucket bucket name
     * @param key file key
     * @return the input stream of the file
     * @throws IOException if the file does not exist
     */
    InputStream read(String bucket, String key) throws IOException;

    /**
     * Read a file fragment from the cache
     * @param bucket bucket name
     * @param key file key
     * @param start fragment start position
     * @param stop fragment stop position
     * @return the input stream of the file fragment
     * @throws IOException if the file does not exist
     */
    InputStream read(String bucket, String key, long start, long stop) throws IOException;

    /**
     * Delete a file from the cache
     * @return true if the file was deleted, false otherwise
     */
    boolean delete(String bucket, String key);

    /**
     * Get the manager's store option type
     * @return the store option type
     */
    StoreOptionType getStoreOptionType();

    ArrayList<CacheFile> getCacheFiles();

    long getCacheBytes();

    long getCacheFilesCount();

    boolean isActive();
}
