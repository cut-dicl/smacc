package edu.cut.smacc.server.tier.result;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.io.InputStreamCacheSplitter;
import edu.cut.smacc.server.cache.common.io.SpecialInputStream;

import java.io.InputStream;
import java.util.List;

public class GetResult extends OperationResultBase {
    private InputStream inputStream;
    private CacheFile hitCacheFile;
    private boolean cacheHit = false;

    public GetResult() {
        super();
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
        if (inputStream instanceof InputStreamCacheSplitter splitter) {
            // miss files
            cacheFiles = splitter.getCacheFiles();
            s3File = splitter.getS3CacheFile();
        } else if (inputStream instanceof SpecialInputStream specialInputStream) {
            hitCacheFile = specialInputStream.getCacheFile();
            cacheHit = true;
        }
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public List<CacheFile> getMissCacheFiles() {
        // The cache files that were created are the ones that were missed
        return getCacheFiles();
    }

    public boolean isCacheHit() {
        return cacheHit;
    }

    public CacheFile getHitFile() {
        return hitCacheFile;
    }

    /**
     * Get size of result. If unknown, returns -1.
     * 
     * @return
     */
    public long getSize() {
        if (cacheHit)
            return hitCacheFile.getActualSize();
        else if (s3File != null)
            return s3File.getActualSize();
        else
            return -1;
    }
}
