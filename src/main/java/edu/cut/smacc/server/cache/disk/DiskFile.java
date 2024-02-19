package edu.cut.smacc.server.cache.disk;

import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.common.io.MultiBlockInputStream;
import edu.cut.smacc.server.cache.common.io.MultiBlockOutputStream;
import edu.cut.smacc.server.cache.common.io.SpecialInputStream;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides methods to access a Disk File which actually is a set of blocks in a disk volume
 *
 * @author Theodoros Danos
 */
public class DiskFile extends CacheFileBase {

    private static final Logger logger = LogManager.getLogger(DiskFile.class);

    /* Instance */
    private final int diskNumber;
    private final UsageStats diskStats;
    private boolean cameFromRecovery = false;

    DiskFile(String bucket, String key, StoreSettings settings, int diskNumber, DiskManager diskManager) {
        super(bucket, key, settings);
        this.settings = settings;
        this.diskNumber = diskNumber;
        this.cacheManager = diskManager;
        this.diskStats = settings.getStats();
        if (logger.isDebugEnabled()) logger.info("[D]INCOMPLETE:" + key);
    }

    //this constructor is secondary constructor and is used in recovery only
    DiskFile(StoreSettings settings, int diskNumber, DiskManager diskManager, FilenameFeatureExtractor features) {
        super(features.getBucket(), features.getKey(), settings);
        state = features.getState();
        this.diskNumber = diskNumber;
        this.cacheManager = diskManager;
        this.diskStats = settings.getStats();
        setVersion(features.getVersion());    // set version at the end of the constructor
    }

    void recoverBlock(File blockFile) {
        cameFromRecovery = true;
        FilenameFeatureExtractor mainFileFeatures = new FilenameFeatureExtractor(blockFile.getName());
        FileBlock newBlock = new FileBlock(mainFileFeatures, state, settings, this);
        reservedRanges.add(mainFileFeatures.getRange());
        fileBlocks.add(newBlock);
        /* in this case file is either complete without being partial, or it is pushed (which by default can't be partial file) */
        isPartialFile = mainFileFeatures.isPartial();
    }

    int getDiskNumber() {
        return this.diskNumber;
    }

    public void delete() {
        deleteFlag = true;
        synchronized (readCounter) {
            setObsoleteBlocks();
            state = StateType.OBSOLETE;    //has to be synchronized set, because of CompleteState() and StateToBePushed()
            deleteFile();
        }
    }

    private long findLastIndex() {
        long max = 0;
        synchronized (fileBlocks) {
            for (CacheBlock block : fileBlocks) {
                if (block.getRange().getStop() > max)
                    max = block.getRange().getStop();
            }
        }
        return max;
    }

    /**
     * get incomplete input stream because of out of memory - incomplete data will be sent to s3
     */

    public InputStream getInputStream() throws IOException {
        synchronized (readCounter) {
            fileAccessCount.incrementAndGet();
            lastUsed = System.currentTimeMillis();
            if (!deleteFlag) {
                readCounter.incrementAndGet();
                if (!isPartialFile) {
                    return new SpecialInputStream(fileBlocks.get(0).getFileInputStream(), this);
                } else if (isFullFile()) {
                    long stopsAt;
                    if (actualSize > 0)        // We have the actual size of file
                    {
                        stopsAt = actualSize - 1;
                    } else {
                        //	We don't have the actual size of file (file recovered)
                        stopsAt = findLastIndex();
                        // 	There is no problem feeding multiBlockInputStream with the stop index and start
                        //	index because we know the in between blocks are available. Because the inRange() has to be called before calling getInputStream
                    }

                    return new SpecialInputStream(new MultiBlockInputStream(0, stopsAt, fileBlocks), this);
                } else if (cameFromRecovery) /* In case of reading from a recovered pushed file in order to upload to s3 */ {
                    long stopsAt = findLastIndex();
                    return new SpecialInputStream(new MultiBlockInputStream(0, stopsAt, fileBlocks), this);
                }
            }
            return null;
        }
    }

    public InputStream getInputStream(long start, long stop) throws IOException {
        synchronized (readCounter) {
            if (fileAccessCount == null)
                fileAccessCount = new AtomicInteger(0);

            fileAccessCount.incrementAndGet();
            lastUsed = System.currentTimeMillis();
            if (!inRange(start, stop)) {
                if (logger.isDebugEnabled()) logger.info("Requested file not in range");
                return null;
            }

            if (!deleteFlag) {
                readCounter.incrementAndGet();
                return new SpecialInputStream(new MultiBlockInputStream(start, stop, fileBlocks), this);
            } else {
                logger.error("Requesting input stream from a marked deleted file");
                return null;
            }
        }
    }

    public MultiBlockOutputStream getOutputStream() {
        fileAccessCount.incrementAndGet();
        lastUsed = System.currentTimeMillis();
        return new MultiBlockOutputStream(0, -1, this, diskStats);
    }

    public MultiBlockOutputStream getOutputStream(long start, long stop) {
        fileAccessCount.incrementAndGet();
        isPartialFile = true;
        lastUsed = System.currentTimeMillis();
        return new MultiBlockOutputStream(start, stop, this, diskStats);
    }

    public boolean isComplete() {
        return state == StateType.COMPLETE;
    }

    @Override
    public StoreOptionType getStoreOption() {
        return StoreOptionType.DISK_ONLY;
    }

}
