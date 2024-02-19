package edu.cut.smacc.server.main.request;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.protocol.HeaderServer;
import edu.cut.smacc.server.tier.result.DeleteResult;

import java.io.IOException;

/**
 * Request handler for SMACC DELETE requests.
 */
public class DeleteRequestHandler extends RequestHandlerBase {

    protected DeleteRequestHandler(ClientConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    public void handleRequest(HeaderServer header, CloudInfo cloudInfo) throws IOException {
        long startTime = System.currentTimeMillis();

        String bucket = header.getBucket();
        String key = header.getKey();

        DeleteResult result = tier.delete(bucket, key, cloudInfo);
        if (result.wasDeletedSuccessfully()) {
            connectionHandler.sendSuccessMessage();
            long deleteTime = System.currentTimeMillis() - startTime;
            timeStatUpdater.updateOnDelete(0, deleteTime);
            for (CacheFile cacheFile : result.getCacheFiles()) {
                if (cacheFile.getStoreOption() == StoreOptionType.MEMORY_ONLY) {
                    memoryStatUpdater.updateOnDelete(cacheFile.getTotalSize(), deleteTime);
                } else if (cacheFile.getStoreOption() == StoreOptionType.DISK_ONLY) {
                    diskStatUpdater.updateOnDelete(cacheFile.getTotalSize(), deleteTime);
                }
            }
            if (result.getS3File() != null) {
                s3StatUpdater.updateOnDelete(result.getS3File().getActualSize(), deleteTime);
            }
        } else {
            connectionHandler.sendErrorMessage("File not found!");
        }
    }
}
