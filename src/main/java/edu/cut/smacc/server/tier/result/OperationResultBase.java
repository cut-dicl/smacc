package edu.cut.smacc.server.tier.result;

import edu.cut.smacc.server.cache.common.CacheFile;

import java.util.ArrayList;
import java.util.List;

public abstract class OperationResultBase implements OperationResult {

    protected List<CacheFile> cacheFiles;
    protected CacheFile s3File;

    public OperationResultBase() {
    }

    @Override
    public List<CacheFile> getCacheFiles() {
        if (cacheFiles == null)
            cacheFiles = new ArrayList<>(0);
        return cacheFiles;
    }

    public CacheFile getS3File() {
        return s3File;
    }

}
