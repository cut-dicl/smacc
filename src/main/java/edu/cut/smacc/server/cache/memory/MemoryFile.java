package edu.cut.smacc.server.cache.memory;

import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.common.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provide a way to handle the complexity of using blocks transparently.
 * This class provides a way to handle an object being in memory
 *
 * @author Theodoros Danos
 */
public class MemoryFile extends CacheFileBase { //shall we let this handled by MemoryManager only or make it available (visible) to the tierManager?
    /* Static */
    private static final Logger logger = LogManager.getLogger(MemoryFile.class);

    /* Instance */
    private final ByteBufferPool pool;

    MemoryFile(ByteBufferPool pool, String bucket, String key, StoreSettings settings,
               MemoryManager memorymgr, UsageStats parentStats) {
        super(bucket, key, settings);
        this.pool = pool;
        this.cacheManager = memorymgr;
        this.parentStats = parentStats;
        if (logger.isDebugEnabled()) logger.info("[M]INCOMPLETE:" + key);
    }

    public void delete() {
        deleteFlag = true;
        synchronized (readCounter) {
            state = StateType.OBSOLETE; //has to be synchronized set, because of ComleteState() and StateToBePushed()
            setObsoleteBlocks();
            deleteFile();
        }
    }

    @Override
    public StoreOptionType getStoreOption() {
        return StoreOptionType.MEMORY_ONLY;
    }

    // Get complete input stream
    public InputStream getInputStream() throws IOException {
        synchronized (readCounter) {
            fileAccessCount.incrementAndGet();
            lastUsed = System.currentTimeMillis();
            if (!deleteFlag) {
                readCounter.incrementAndGet();
                if (!isPartialFile) {
                    return new SpecialInputStream(fileBlocks.get(0).getFileInputStream(), this);
                } else if (isFullFile()) {
                    long endsAt = actualSize - 1;
                    return new SpecialInputStream(new MultiBlockInputStream(0, endsAt, fileBlocks), this);
                }
            }
            return null;
        }
    }

    public InputStream getInputStream(long start, long stop) throws IOException {
        synchronized (readCounter) {
            fileAccessCount.incrementAndGet();
            lastUsed = System.currentTimeMillis();
            if (!inRange(start, stop)) return null;

            if (!deleteFlag) {
                readCounter.incrementAndGet();
                return new SpecialInputStream(new MultiBlockInputStream(start, stop, fileBlocks), this);
            }
            return null;
        }
    }

    public MultiBlockOutputStream getOutputStream() {
        fileAccessCount.incrementAndGet();
        lastUsed = System.currentTimeMillis();
        return new MultiBlockOutputStream(0, -1, this, pool, parentStats);
    }

    public MultiBlockOutputStream getOutputStream(long start, long stop) {
        fileAccessCount.incrementAndGet();
        isPartialFile = true;
        lastUsed = System.currentTimeMillis();
        return new MultiBlockOutputStream(start, stop, this, pool, parentStats);
    }

}