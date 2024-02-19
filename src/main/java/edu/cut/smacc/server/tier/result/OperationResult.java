package edu.cut.smacc.server.tier.result;

import edu.cut.smacc.server.cache.common.CacheFile;

import java.util.List;

/**
 * Interface for operation results.
 */
public interface OperationResult {

    /**
     * Returns the cache files that were affected by the operation (put, get, delete).
     */
    List<CacheFile> getCacheFiles();

}
