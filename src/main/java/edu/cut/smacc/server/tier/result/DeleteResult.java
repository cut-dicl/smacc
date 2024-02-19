package edu.cut.smacc.server.tier.result;

import java.util.ArrayList;

import edu.cut.smacc.server.cache.common.CacheFile;

public class DeleteResult extends OperationResultBase {
    private boolean deletedSuccessfully = false;

    public DeleteResult() {
        super();
        this.cacheFiles = new ArrayList<>();
    }

    public void setSuccessfulDeletion() {
        this.deletedSuccessfully = true;
    }

    public boolean wasDeletedSuccessfully() {
        return deletedSuccessfully;
    }

    public void addCacheFile(CacheFile cacheFile) {
        this.cacheFiles.add(cacheFile);
    }

    public void setS3File(CacheFile s3File) {
        this.s3File = s3File;
    }
}
