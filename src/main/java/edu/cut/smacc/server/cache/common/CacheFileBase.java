package edu.cut.smacc.server.cache.common;

import edu.cut.smacc.server.cache.common.io.SpecialInputStream;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import edu.cut.smacc.server.cache.disk.DiskFile;
import edu.cut.smacc.server.cache.memory.MemoryFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An abstract class to handle the common operations of the cache files
 */
public abstract class CacheFileBase implements CacheFile {

    private static final Logger logger = LogManager.getLogger(CacheFileBase.class);

    protected String bucket;
    protected String key;
    protected StateType state; // 0 = incomplete, 1 = to be pushed, 2 = Complete, 3 = Obsolete
    protected final AtomicInteger readCounter;    //how many readers are currently reading
    protected long version = 0;    //incomplete files have version 0 - only when they are not incomplete are versioned
    protected boolean deleteFlag = false;

    protected CacheManagerBase cacheManager;

    protected StoreSettings settings;
    protected boolean hasBeenDeleted = false;
    protected final List<CacheBlock> fileBlocks;
    protected final List<CacheBlock> fileIncompleteBlocks;
    protected final List<BlockRange> reservedRanges;
    protected long actualSize = 0;
    protected boolean isPartialFile = false;    //is it partial or continuous
    protected long lastS3Modification;
    protected UsageStats parentStats;
    protected long lastUsed = System.currentTimeMillis();
    protected AtomicInteger fileAccessCount;


    public CacheFileBase(String bucket, String key, StoreSettings settings) {
        this.bucket = bucket;
        this.key = key;
        this.settings = settings;
        this.state = StateType.INCOMPLETE;
        this.readCounter = new AtomicInteger(0);
        this.fileAccessCount = new AtomicInteger(0);
        fileBlocks = Collections.synchronizedList(new LinkedList<>());
        fileIncompleteBlocks = Collections.synchronizedList(new LinkedList<>());
        reservedRanges = Collections.synchronizedList(new LinkedList<>());
    }

    @Override
    public boolean isPartialFile() {
        return isPartialFile;
    }

    @Override
    public void setPartialFile() {
        isPartialFile = true;
    }

    @Override
    public long getVersion() {
        return this.version;
    }

    @Override
    public void setVersion(long version) {
        if (this.version == 0)    //initial value
        {
            this.version = version;
            if (logger.isDebugEnabled()) {
                if (this instanceof MemoryFile) {
                    logger.info("MEMORY VERSION " + version);
                } else if (this instanceof DiskFile) {
                    logger.info("DISK VERSION " + version);
                }
            }
            synchronized (fileBlocks) {
                for (CacheBlock block : fileBlocks)
                    block.setVersion(version);
            }
        }
    }

    @Override
    public void abortWrite() {
        if (state != StateType.INCOMPLETE) {
            if (this instanceof MemoryFile) {
                logger.error("Aborting [M]file (client disconnected) although file is in state " +
                        "different that incomplete - should never happen");
            } else if (this instanceof DiskFile) {
                logger.error("Aborting [D]file (client disconnected) although file is in state " +
                        "different that incomplete - should never happen");
            }
        }
        state = StateType.OBSOLETE;
        for (CacheBlock block : fileIncompleteBlocks) {
            block.abortWrite();
        }
    }

    @Override
    public CacheType type() {
        return (this instanceof MemoryFile) ? CacheType.MEMORY_FILE : CacheType.DISK_FILE;
    }

    @Override
    public long getSize() {
        long totalSize = 0;
        for (CacheBlock block : fileBlocks) {
            totalSize += block.getSize();
        }
        return totalSize;
    }

    @Override
    public long getTotalSize() { //completed + incomplete blocks
        long totalSize = 0;
        for (CacheBlock block : fileBlocks) {
            totalSize += block.getSize();
        }
        if (!fileIncompleteBlocks.isEmpty()) {
            for (CacheBlock block : fileIncompleteBlocks) {
                totalSize += block.getSize();
            }
        }
        return totalSize;
    }

    @Override
    public void decReadQueue(InputStream in) throws IOException {
        synchronized (readCounter) {
            readCounter.decrementAndGet(); //decrease readers counter - Could be used AtomicInteger, but I prefer this way
            if (in != null)
                in.close(); //make sure to close the file, within synchronized block, not letting anyone move the file while closing it
        }
        if (readCounter.intValue() == 0 && isObsolete())
            delete();    //when there are no readers and marked for deletion, delete
        //it's ok to not be syn, because when its obsolete, no read can happen from DiskManager.
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getBucket() {
        return bucket;
    }

    @Override
    public void lockReader() { //is used in downgrade. Is needed because we read blocks directly
        synchronized (readCounter) {
            readCounter.incrementAndGet();
        }
    }

    /**
     * Size of actual size on s3 (despite the size of file in cache, the s3 file will always be complete and will have the actual size of the uploaded file)
     */
    @Override
    public long getActualSize() {
        return actualSize;
    }

    /**
     * get incomplete input stream because of out of memory - incomplete data will be sent to s3
     */
    @Override
    public InputStream getIncompleteInputStream() throws IOException {
        synchronized (readCounter) {
            lastUsed = System.currentTimeMillis();
            if (fileBlocks.size() > 0) {
                readCounter.incrementAndGet();
                return new SpecialInputStream(fileBlocks.get(0).getFileInputStream(), this);
            } else return null;
        }
    }

    @Override
    public void deleteIfIncomplete() {
        if (isIncomplete())
            delete();
    }

    @Override
    public void stateComplete() {
        synchronized (readCounter) {   //this actually does not synchronize with Read but with delete() that is also synchronized with ReadQueue
            if (!this.deleteFlag && !isObsolete()) {
                if (!isComplete()) {
                    completeBlocks();
                    if (logger.isDebugEnabled()) {
                        if (this instanceof MemoryFile)
                            logger.info("MEMORY COMPLETE[" + version + "]:" + getKey());
                        else if (this instanceof DiskFile)
                            logger.info("DISK COMPLETE[" + version + "]:" + getKey());
                    }
                    //MAKE IT GLOBALLY VISIBLE
                    if (state == StateType.INCOMPLETE)
                        makeVisible();
						/*State==0: This is needed because we want to put it in mapping only if it is NOT coming through push, otherwise it will delete itself
						and same time being in map!*/
                    this.state = StateType.COMPLETE;        //WARNING: Change state only after making visible and only after renaming
                } else {
                    if (logger.isDebugEnabled()) {
                        if (this instanceof MemoryFile)
                            logger.info("MEMORY COMPLETE[" + version + "]:" + getKey());
                        else if (this instanceof DiskFile)
                            logger.info("DISK COMPLETE[" + version + "]:" + getKey());
                    }
                    completeBlocks();
                }
            } else {
                if (logger.isDebugEnabled()) {
                    if (this instanceof MemoryFile)
                        logger.info("Memory File discarded before completion");
                    else if (this instanceof DiskFile)
                        logger.info("Disk File discarded before completion");
                }
            }
        }
    }

    @Override
    public void stateToBePushed() {
        synchronized (readCounter) {
            //this actually does not synchronize with Read but with delete() that is also synchronized with ReadQueue
            //nobody should be able to delete the file before push (only the CacheOutputStream can delete file before push, but it will never delete and then push.)
            if (this.deleteFlag || isObsolete()) {
                if (this instanceof MemoryFile) {
                    logger.error("Pushing [M]file although file is marked for deletion - should never happen");
                } else if (this instanceof DiskFile) {
                    logger.error("Pushing [D]file although file is marked for deletion - should never happen");
                }
                return;
            }
            if (fileBlocks.size() == 0) {
                logger.fatal("Pushing file without any block - this should never happen");
                return;
            }
            synchronized (fileBlocks) {
                CacheBlock block = fileBlocks.get(0);
                block.toBePushed();
            }
            if (logger.isDebugEnabled()) {
                if (this instanceof MemoryFile)
                    logger.info("MEMORY PUSHED[" + version + "]:" + getKey());
                else if (this instanceof DiskFile)
                    logger.info("DISK PUSHED[" + version + "]:" + getKey());
            }
            //MAKE IT GLOBALLY VISIBLE
            state = StateType.TOBEPUSHED;    //WARNING: Change state only after renaming
            makeVisible();
        }
    }

    @Override
    public void addIncompleteBlock(CacheBlock block) {
        synchronized (fileIncompleteBlocks) {
            if (isObsolete()) block.delete();
            fileIncompleteBlocks.add(block);
        }
    }

    @Override
    public void makeBlockVisible(CacheBlock block) {
        if (isObsolete()) return;
        synchronized (fileIncompleteBlocks) {
            fileIncompleteBlocks.remove(block);
        }
        synchronized (fileBlocks) {
            fileBlocks.add(block);
        }
    }

    @Override
    public void replaceReservedRange(BlockRange prevRange, BlockRange newRange) {
        synchronized (reservedRanges) {
            Iterator<BlockRange> iter = reservedRanges.iterator();
            while (iter.hasNext()) {
                BlockRange trange = iter.next();
                if (trange.equals(prevRange)) {
                    iter.remove();
                    break;
                }
            }
            reservedRanges.add(newRange);
        }
    }

    @Override
    public List<BlockRange> getReservedRanges() {
        return reservedRanges;
    }

    @Override
    public void setActualSize(long size) {
        actualSize = size;
    }

    @Override
    public boolean inRange(long start, long stop) {
        List<BlockRange> currentRanges = new ArrayList<>(fileBlocks.size());
        for (CacheBlock block : fileBlocks) {
            currentRanges.add(block.getRange());
        }

        ArrayList<BlockRange> unifiedList = new MultiBlockUnifier(currentRanges).getUnifiedList();

        for (BlockRange blockRange : unifiedList)
            if (blockRange.contains(start) && blockRange.contains(stop)) return true;

        return false;
    }

    @Override
    public List<CacheBlock> getCacheBlocks() {
        return fileBlocks;
    }

    @Override
    public void setLastModified(long date) {
        lastS3Modification = date;
    }

    @Override
    public long getLastModified() {
        return lastS3Modification;
    }

    @Override
    public long getLastTimeUsed() {
        return lastUsed;
    }

    @Override
    public StoreSettings getSettings() {
        return settings;
    }

    /**
     * Does the file has all the contents of the actual file? (partial or in single block doesn't matter)
     *
     * @return true if the file is full
     */
    @Override
    public boolean isFullFile() {
        long completeSize = 0;
        for (CacheBlock block : fileBlocks) {
            if (block.isComplete() || block.isPushed())
                completeSize += block.getSize();
        }
        return completeSize == actualSize;
    }

    @Override
    public boolean isComplete() {
        return state == StateType.COMPLETE;
    }

    @Override
    public boolean isPushed() {
        return state == StateType.TOBEPUSHED;
    }

    @Override
    public boolean isObsolete() {    //file HAS BEEN deleted (file no longer exist - this object is obsolete)
        return state == StateType.OBSOLETE;
    }

    @Override
    public StateType getState() {
        return state;
    }

    @Override
    public List<BlockRange> getBlockRangeList() {
        return new ArrayList<>(reservedRanges);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CacheFile other) {
            return key.equals(other.getKey());
        }
        return false;
    }

    protected void makeVisible() {
        cacheManager.put(bucket, key, this);
    }

    protected void setObsoleteBlocks() {
        if (!isObsolete()) {
            for (CacheBlock block : fileBlocks) { //we dont need synchronization because when DiskFile is obsolete incomplete blocks remain incomplete
                block.setStateFileObsolete();
            }

            for (CacheBlock block : fileIncompleteBlocks) {//we dont need synchronization because when DiskFile is obsolete incomplete blocks remain incomplete
                block.setStateFileObsolete();
            }
        }
    }

    protected void deleteFile() {
        if (readCounter.intValue() == 0) {
            if (!hasBeenDeleted) { //file can be deleted only once
                hasBeenDeleted = true;
                if (logger.isDebugEnabled()) {
                    if (this instanceof MemoryFile) {
                        logger.info("MEMORY-DELETING FILE:" + getKey() + " V:[" + version + "]");
                    } else if (this instanceof DiskFile) {
                        logger.info("DISK-DELETING FILE:" + getKey() + " V:[" + version + "]");
                    }
                }
                //Delete completed files
                for (CacheBlock block : fileBlocks) { //we dont need synchronization because when DiskFile is obsolete incomplete blocks remain incomplete
                    block.delete();
                }
                //Delete incomplete files - Incomplete files are deleted when they close the output stream - If they never close, they are collected by the next startup
                synchronized (fileIncompleteBlocks) {
                    Iterator<CacheBlock> iter = fileIncompleteBlocks.iterator();
                    while (iter.hasNext()) {
                        CacheBlock block = iter.next();
                        block.delete();
                        iter.remove();
                    }
                }
            }
        }
    }

    private boolean isIncomplete() {
        return state == StateType.INCOMPLETE;
    }

    private void completeBlocks() {
        synchronized (fileBlocks) {
            for (CacheBlock block : fileBlocks) {
                if (!block.isComplete()) block.complete();
            }
        }
    }

    @Override
    public int compareTo(CacheFile o) {
        // Default comparison order: size, key
        if (this.getActualSize() < o.getSize())
            return -1;
        else if (this.getActualSize() > o.getSize())
            return 1;

        return this.getKey().compareTo(o.getKey());
    }

}
