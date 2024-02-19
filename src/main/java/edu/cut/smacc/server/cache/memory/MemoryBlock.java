package edu.cut.smacc.server.cache.memory;

import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.common.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Memory block is a block of data bing on memory (data are part of an object)
 *
 * @author Theodoros Danos
 */
public class MemoryBlock implements CacheBlock {
    /* Static */
    private static final Logger logger = LogManager.getLogger(MemoryBlock.class);

    /* Instance */
    private String stateFolder;
    private BlockRange range;
    private String bucket;
    private String key;
    private ByteBufferQueue blockQueue;
    private File blockStateFile;
    private long version;
    private StateType state;
    private long writeSoFar = 0;
    private ByteBufferQueueOutputStream outputStream;
    private boolean isDeleted = false;
    private ByteBufferPool pool;
    private UsageStats parentStats;
    private CacheFile cf;
    private boolean isClosed = false;

    public MemoryBlock(long start, long stop, String stateFolder, ByteBufferPool pool, CacheFile cf, UsageStats parentStats) {
        this.parentStats = parentStats;
        this.pool = pool;
        UsageStats stats = new UsageStats();
        blockQueue = new ByteBufferQueue(stats);
        outputStream = new ByteBufferQueueOutputStream(pool, blockQueue);
        range = new BlockRange(start, stop);
        this.stateFolder = stateFolder;
        this.bucket = cf.getBucket();
        this.key = cf.getKey();
        this.version = cf.getVersion();
        this.cf = cf;

        state = StateType.INCOMPLETE;
    }

    private String generateStateFilename(StateType setState) {
        StringBuilder sb = new StringBuilder();

        sb.append(stateFolder);
        switch (setState) {
            case COMPLETE -> sb.append("COMPLETE$");
            case INCOMPLETE -> sb.append("INCOMPLETE$");
            case OBSOLETE -> sb.append("OBSOLETE$");
            case TOBEPUSHED -> sb.append("PUSHED$");
            default -> throw new IllegalArgumentException("Unexpected value: " + setState);
        }

        sb.append(version);
        sb.append("##");
        sb.append(StringShort.toHex(bucket));
        sb.append("#");
        sb.append(StringShort.toHex(key));
        sb.append("-");
        sb.append(range.getStart());
        sb.append("-");
        sb.append(range.getStop());

        return sb.toString();
    }

    public void setStateFileObsolete() {
        StateType prevState = state;
        if (prevState != StateType.INCOMPLETE) {
            String previousfile = blockStateFile.getAbsolutePath();
            state = StateType.OBSOLETE;
            String newfile = generateStateFilename(StateType.OBSOLETE);

            try {
                Files.move(Paths.get(previousfile), Paths.get(newfile), REPLACE_EXISTING);
            } catch (IOException e) {
                logger.error("Could not rename state file", e);
            }//LOG EVENT and continue
            blockStateFile = new File(newfile);
        }
    }

    public void abortWrite() {
        outputStream.close();
        uncheckedDelete();
    }

    private void deleteStateFile() {
        if (!isIncomplete()) {
            if (blockStateFile != null && blockStateFile.exists())
                blockStateFile.delete();    //delete statefile if not incomplete (there are no incomplete state files)
        }
    }

    public void complete() {
        try {
            logger.debug("Memory BLock Complete");

            String stateFilename;

            //CREATE/RENAME STATE FILE
            if (state == StateType.INCOMPLETE)    //if incomplete - then create complete state file
            {
                stateFilename = generateStateFilename(StateType.COMPLETE);
                blockStateFile = new File(stateFilename);
                blockStateFile.createNewFile();
            } else if (state == StateType.TOBEPUSHED) {    //if from pushed changed to complete, then rename the file
                File newstateFile = new File(generateStateFilename(StateType.COMPLETE));
                Files.move(Paths.get(blockStateFile.getAbsolutePath()), Paths.get(newstateFile.getAbsolutePath()), REPLACE_EXISTING);
                blockStateFile = newstateFile;
            }

            this.state = StateType.COMPLETE;

        } catch (Exception e) {
            logger.error("Could not complete memory block. " + e.getMessage());
        }
    }

    public void toBePushed() {
        try {
            //CREATE STATE FILE
            String statefile = generateStateFilename(StateType.TOBEPUSHED);
            blockStateFile = new File(statefile);
            blockStateFile.createNewFile();

            this.state = StateType.TOBEPUSHED;

        } catch (IOException e) {
            logger.error("Memory Pushed Error: " + e.getMessage());
        }
    }

    public InputStream getFileInputStream() {
        return new ByteBufferQueueInputStream(blockQueue);
    }

    public long getSize() {
        return range.getLength();
    }

    public void write(byte[] buffer, int offset, int len) throws IOException {
        outputStream.write(buffer, offset, len);
        writeSoFar += len;
    }

    public void write(int c) throws IOException {
        outputStream.write(c);
        writeSoFar += 1;
    }

    public long available() {
        return range.getLength() - writeSoFar;
    }

    public boolean close() {
        boolean isObs;

        if (range.getStop() == -1) {
            range.update(range.getStart(), writeSoFar - 1);
        }

        synchronized (this) {
            outputStream.close();
            isClosed = true;
            isObs = isObsolete();
        }

        if (isObs) {
            delete();
            return false;
        } else if (available() == 0)
            return true;
        else {
            BlockRange newRange = new BlockRange(range.getStart(), range.getStart() + writeSoFar - 1);
            BlockRange prevRange = range;
            cf.replaceReservedRange(prevRange, newRange);
            range = newRange;
            logger.debug("Premature Closing Block - Available: " + available() + "new Range: " + newRange.toString());
            return true;
        }
    }

    public void delete() {
        if (!isDeleted) {
            if (isIncomplete()) {
                synchronized (this) {
                    if (!isClosed)
                        this.state = StateType.OBSOLETE;
                    else
                        uncheckedDelete();
                }
            } else {
                uncheckedDelete();
            }
        }
    }

    private void uncheckedDelete() {
        if (logger.isDebugEnabled()) logger.info("DELETE: Memory block");
        isDeleted = true;
        blockQueue.delete(pool);
        deleteStateFile();
        parentStats.decrement(writeSoFar, writeSoFar);
    }

    public BlockRange getRange() {
        return range;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public boolean isComplete() {
        return state == StateType.COMPLETE;
    }

    public boolean isPushed() {
        return state == StateType.TOBEPUSHED;
    }

    public boolean isIncomplete() {
        return state == StateType.INCOMPLETE;
    }

    public boolean isObsolete() {
        return state == StateType.OBSOLETE;
    }

    public StateType getState() {
        return state;
    }
}