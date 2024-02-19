package edu.cut.smacc.server.tier;

import edu.cut.smacc.server.cache.common.CacheBlock;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.io.SpecialInputStream;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.disk.DiskManager;
import edu.cut.smacc.configuration.ServerConfigurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * This thread starts when a downgration task is given and handles the downgration of a file
 *
 * @author Theodoros Danos
 */
public class DowngrationRunner implements Runnable {

    private static final Logger logger = LogManager.getLogger(DowngrationRunner.class);

    private final CacheFile evictedFile;
    private final TierManager tierManager;
    private final DiskManager dMgr;
    private final UsageStats downgradeStats;

    DowngrationRunner(CacheFile evictedFile, TierManager tier, UsageStats downgradeStats) {
        this.evictedFile = evictedFile;
        this.tierManager = tier;
        this.dMgr = tierManager.getDiskManager();
        this.downgradeStats = downgradeStats;
        downgradeStats.increment(evictedFile.getTotalSize());
    }

    public void run() {
        if (logger.isDebugEnabled()) logger.info("Downgration task started");
        //if disk is out of space too, then the CacheOutputStream object will handle it
        String bucket = evictedFile.getBucket(), key = evictedFile.getKey();
        CacheFile newDiskFile;
        CacheOutputStream newOut;
        InputStream in;
        long evictedFileVersion = evictedFile.getVersion();
        CacheFile currentDiskFile = dMgr.getFile(bucket, key);
        long evictedFileTotalSize = evictedFile.getTotalSize();

        if (currentDiskFile != null && currentDiskFile.getVersion() >= evictedFileVersion) {
            evictedFile.delete();
            downgradeStats.decrement(evictedFileTotalSize, evictedFileTotalSize);
            return; //having the same version it means the disk has exactly the same data as memory. If disk has greater version it means has a newer file than memory.
        }

        try {
            //the version of disk file has not the same version of disk, so we create a new one
            if (evictedFile.isPartialFile()) {
                try {
                    List<CacheBlock> blocks = evictedFile.getCacheBlocks();
                    newDiskFile = dMgr.create(bucket, key, evictedFile.getActualSize());
                    newDiskFile.setVersion(evictedFileVersion); //set the version of new created file
                    for (CacheBlock block : blocks) {
                        newOut = new CacheOutputStream(newDiskFile, block.getRange().getStart(), block.getRange().getStop(), tierManager);
                        newOut.setEvictedFile(evictedFile);
                        evictedFile.lockReader();    //increase reader counter
                        in = new SpecialInputStream(block.getFileInputStream(), evictedFile);    //in close will decrease the reader counter
                        try {
                            transferBlock(in, newOut);
                        } catch (IOException e) {
                            logger.error("Could not downgrade block: " + e.getMessage());
                        }
                    }
                } catch (IOException e) {
                    logger.error("Could not create a cache file for downgrading...: " + e.getMessage());
                }
            } else {
                try {
                    newDiskFile = dMgr.create(bucket, key);
                    newDiskFile.setVersion(evictedFileVersion); //set the version of new created file
                    newOut = new CacheOutputStream(newDiskFile, tierManager);
                    newOut.setEvictedFile(evictedFile);
                    in = evictedFile.getInputStream();
                    transferBlock(in, newOut);
                } catch (IOException e) {
                    logger.error("Could not downgrade a cache file: " + e.getMessage());
                }
            }
            evictedFile.delete();
        } catch (Exception e) {
            logger.error("DowngrationRunner has failed: " + e.getMessage());
        }

        downgradeStats.decrement(evictedFileTotalSize, evictedFileTotalSize);
    }

    private void transferBlock(InputStream in, CacheOutputStream out) throws IOException {
        byte[] buff = new byte[ServerConfigurations.getDowngradeBufferSize()];

        try {
            while (in.available() > 0) {
                int r = in.read(buff);
                out.write(buff, 0, r);
            }
            in.close();
            out.close();
        } catch (Exception e)    //make sure to close the input file (in order to unlock the readers counter)
        {
            logger.error("Block Transfer has failed");
            in.close();
            throw new IOException(e);
        }
    }

}
