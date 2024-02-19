package edu.cut.smacc.server.tier.result;

import edu.cut.smacc.server.tier.CacheOutputStream;


public class PutResult extends OperationResultBase {

    private CacheOutputStream cacheOutputStream;

    public PutResult() {
        super();
    }

    public void setCacheOutputStream(CacheOutputStream cacheOutputStream) {
        this.cacheOutputStream = cacheOutputStream;
        cacheFiles = cacheOutputStream.getCacheFiles();
        s3File = cacheOutputStream.getS3CacheFile();
    }

    public CacheOutputStream getCacheOutputStream() {
        return cacheOutputStream;
    }
}
