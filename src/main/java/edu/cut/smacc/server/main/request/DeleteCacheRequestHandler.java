package edu.cut.smacc.server.main.request;

import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.SMACCObject;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.protocol.HeaderServer;
import edu.cut.smacc.server.protocol.RequestType;
import edu.cut.smacc.server.tier.result.DeleteResult;

import java.io.IOException;
import java.util.List;

/**
 * Request handler for SMACC DELETE(from)CACHE requests.
 */
public class DeleteCacheRequestHandler extends RequestHandlerBase {

    protected DeleteCacheRequestHandler(ClientConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    public void handleRequest(HeaderServer header, CloudInfo cloudInfo) throws IOException {
        String bucket = header.getBucket();
        String key = header.getKey();
        if (header.getRequestType() == RequestType.CLEAR_CACHE) {
            bucket = ServerConfigurations.getDefaultBucket();
            key = null;
        }

        List<SMACCObject> list = tier.list(bucket, key);
        for (SMACCObject object : list) {
            long startTime = System.currentTimeMillis();
            DeleteResult result = tier.deleteFileFromCache(bucket, object.getKey());
            if (!result.wasDeletedSuccessfully()) {
                connectionHandler.sendErrorMessage("Could not delete cache file");
                return;
            }
            long deleteTime = System.currentTimeMillis() - startTime;
            for (CacheFile cacheFile : result.getCacheFiles()) {
                if (cacheFile.getStoreOption() == StoreOptionType.MEMORY_ONLY) {
                    memoryStatUpdater.updateOnDelete(cacheFile.getTotalSize(), deleteTime);
                } else {
                    diskStatUpdater.updateOnDelete(cacheFile.getTotalSize(), deleteTime);
                }
                timeStatUpdater.updateOnDelete(cacheFile.getTotalSize(), deleteTime);
            }
        }
        connectionHandler.sendSuccessMessage();
    }
}
