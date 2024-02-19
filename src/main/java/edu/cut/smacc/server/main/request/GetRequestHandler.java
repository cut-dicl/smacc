package edu.cut.smacc.server.main.request;

import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.BlockRange;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.protocol.HeaderServer;
import edu.cut.smacc.server.tier.result.GetResult;

import java.io.IOException;
import java.io.InputStream;

/**
 * Request handler for SMACC GET requests.
 */
public class GetRequestHandler extends RequestHandlerBase {

    protected GetRequestHandler(ClientConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    public void handleRequest(HeaderServer header, CloudInfo cloudInfo) throws IOException {
        long startTime = System.currentTimeMillis();

        String bucket = header.getBucket();
        String key = header.getKey();
        BlockRange range = header.getRange();

        InputStream in;
        GetResult result;

        try {
            /* Read from Cache or S3 */
            if (range == null) {
                result = tier.read(bucket, key, cloudInfo);
            } else {
                result = tier.read(bucket, key, range.getStart(), range.getStop(), cloudInfo);
            }
            in = result.getInputStream();
        } catch (IOException e) {
            /* File or Range not found */
            connectionHandler.sendErrorMessage("File not found (or range does not exist)");
            return;
        } catch (Exception e) {
            /* Cache Error Occurred */
            connectionHandler.sendErrorMessage(e.getMessage());
            throw new IOException(e);
        }

        // Create buffer for placing the read data
        int r;
        byte[] buffer;
        long size = result.getSize();
        if (size > 0 && size < ServerConfigurations.getServerBufferSize())
            buffer = new byte[(int) size];
        else
            buffer = new byte[ServerConfigurations.getServerBufferSize()];

        try {
            connectionHandler.sendSuccessMessage(); // Message is found
            connectionHandler.getDataOutputStream().writeLong(size);
            connectionHandler.getDataOutputStream().flush();

            while (true) {
                try { // Send data
                    r = in.read(buffer);
                } catch (IOException e) { // Send an Errorneus EOF Message
                    logger.error(e.getMessage(), e);
                    in.close();
                    connectionHandler.closeConnection();
                    return;
                }

                if (r > 0) {
                    connectionHandler.getDataOutputStream().write(buffer, 0, r);
                } else {
                    break;
                }
            }

            in.close();
            in = null;

            connectionHandler.getConnection().shutdownOutput();
            connectionHandler.closeConnection();
            long getTime = System.currentTimeMillis() - startTime;
            timeStatUpdater.updateOnGet(0, getTime);

            // CACHE HIT
            if (result.isCacheHit()) {
                if (result.getHitFile().getStoreOption() == StoreOptionType.MEMORY_ONLY) {
                    // Memory hit
                    memoryStatUpdater.updateGetOnHit(result.getHitFile().getActualSize(), getTime);
                } else {
                    // Disk hit, memory miss
                    memoryStatUpdater.updateGetOnMiss(result.getHitFile().getActualSize());
                    diskStatUpdater.updateGetOnHit(result.getHitFile().getActualSize(), getTime);
                }
                return;
            }

            // CACHE MISS
            for (CacheFile cacheFile : result.getMissCacheFiles()) {
                if (cacheFile.getStoreOption() == StoreOptionType.MEMORY_ONLY) {
                    memoryStatUpdater.updateGetOnMiss(cacheFile.getActualSize());
                } else if (cacheFile.getStoreOption() == StoreOptionType.DISK_ONLY) {
                    diskStatUpdater.updateGetOnMiss(cacheFile.getActualSize());
                }
            }
            getTime = System.currentTimeMillis() - startTime;
            if (result.getS3File() != null) {
                s3StatUpdater.updateOnGet(result.getS3File().getActualSize(), getTime);
            }

        } catch (IOException e) {
            if (in != null) in.close();
            connectionHandler.closeConnection();
        }
    }

}
