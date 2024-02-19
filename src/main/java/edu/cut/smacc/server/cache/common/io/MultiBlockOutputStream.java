package edu.cut.smacc.server.cache.common.io;

import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.disk.FileBlock;
import edu.cut.smacc.server.cache.memory.MemoryBlock;
import edu.cut.smacc.configuration.ServerConfigurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

enum MBType {MEMORY, DISK}

/**
 * This class helps to write multiple blocks as needed (write new blocks
 * if there are missing ranges and do nothing if data exists)
 *
 * @author Theodoros Danos
 */
public class MultiBlockOutputStream {
    private static final Logger logger = LogManager.getLogger(MultiBlockOutputStream.class);

    private CacheBlock currentBlock = null;
    private final List<BlockRange> reservedRanges;
    private StoreSettings settings;
    private long currentStart;
    private long currentStop;
    private long currentLength;
    private long finalStop;
    private int bufferAvailable;
    private MBType type;
    private int buffOffset;
    private CacheFile cf;
    private boolean isClose = false;
    private long eosTotalBytes = 0;
    private ByteBufferPool pool;    //used only by MemoryFile & MemoryBlock
    private UsageStats stats;
    private boolean memoryFull = false;
    private byte[] oneByteArray = new byte[1];

    public MultiBlockOutputStream(long start, long stop, CacheFile cf, UsageStats stats) {
        this.reservedRanges = cf.getReservedRanges();
        this.settings = cf.getSettings();
        currentStart = start;
        currentStop = stop;
        finalStop = stop;
        currentLength = 0;
        this.cf = cf;
        this.stats = stats;

        type = MBType.DISK;
    }

    public MultiBlockOutputStream(long start, long stop, CacheFile cf, ByteBufferPool pool, UsageStats stats) {
        this.reservedRanges = cf.getReservedRanges();
        this.settings = cf.getSettings();
        currentStart = start;
        currentStop = stop;
        finalStop = stop;
        currentLength = 0;
        this.cf = cf;
        this.pool = pool;
        this.stats = stats;

        type = MBType.MEMORY;
    }

    void abort() {
        isClose = true;
        cf.abortWrite();
    }

    private boolean isMemoryFull() {
        return memoryFull;
    }

    public CacheFile getCacheFile() {
        return cf;
    }

    private void checkTierCapacity(int len) throws CacheOutOfMemoryException {
        boolean condition;
        if (ServerConfigurations.overflowMemory())
            condition = stats.getReportedUsage() >= stats.getMaxCapacity();
        else
            condition = stats.getReportedUsage() + len >= stats.getMaxCapacity(); //memory still can be overflowed because of parallel writes that are not aware of each other

        if (condition) {
            logger.info("MultiBlockOutputStream: Out of Memory! ( " + stats.getReportedUsage() + "+" + len + " / " + stats.getMaxCapacity() + " )");
            if (currentBlock != null) {
                cf.setPartialFile();
            }
            memoryFull = true;
            throw new CacheOutOfMemoryException("Cache Section Full");    // i can't throw any other exception because of the extension of output stream limitation
        }
    }

    public void write(int c) throws IOException {
        oneByteArray[0] = (byte) c;
        write(oneByteArray, 0, 1);
    }

    public void write(byte[] buff) throws IOException {
        write(buff, 0, buff.length);
    }

    public void write(byte[] buff, int offset, int len) throws IOException {
        if (isMemoryFull()) return;
        if (isClose) throw new IOException("Writing to a closed stream...");

        checkTierCapacity(len);

        bufferAvailable = len;    //buffer Length – availLen =  class member
        buffOffset = offset;    //-> buffOffset = class member
        boolean contin;

        if (currentStop > -1)    //writing partial file
        {
            while (bufferAvailable > 0) {
                if (currentBlock == null) {
                    contin = findValidStartStop();
                    if (!contin) return;
                    currentBlock = newBlock(currentStart, currentStop);
                    cf.addIncompleteBlock(currentBlock);
                    if (currentBlock.isObsolete()) return;
                }

                long blockAvailable = currentBlock.available();
                int writeLen;

                if (blockAvailable > 0) {
                    if (bufferAvailable < blockAvailable) writeLen = bufferAvailable;
                    else
                        writeLen = (int) blockAvailable;    //there is no loss of precision from casting due to condition above
                    //there are different types (long and int) because the file can be size of long but buffer (from write) can only be the size of an int

                    currentBlock.write(buff, buffOffset, writeLen);
                    bufferAvailable = bufferAvailable - writeLen;
                    currentStart += writeLen;
                    eosTotalBytes += writeLen;
                    stats.increment(writeLen);

                    currentBlock.available();
                }

                if (currentBlock.available() == 0) {
                    if (!currentBlock.close()) {
                        logger.error("Our program should write all the data to file Stop:" + finalStop);
                    } else
                        cf.makeBlockVisible(currentBlock);

                    currentBlock = null;
                }
            }
        } else    //writing a continuous file (not partial)
        {
            if (currentBlock == null) {
                currentBlock = newBlock(currentStart, currentStop);
                cf.addIncompleteBlock(currentBlock);
            }

            currentBlock.write(buff, offset, len);
            currentStart += len;
            currentLength += len;
            eosTotalBytes += len;
            stats.increment(len);
        }
    }

    public void close() throws IOException {
        if (!isClose) {
            if (currentStop == -1) {
                currentStop = currentLength - 1;
                if (!memoryFull) finalStop = currentStop;
                long totalLength = currentLength;
                if (!memoryFull) cf.setActualSize(totalLength);
            }

            isClose = true;
            if (currentBlock != null)     //in case the block had remaining (which will trigger it's deletion)
            {
                if (currentBlock.close())
                    cf.makeBlockVisible(currentBlock);

                currentBlock = null;
            }

            logger.debug("Total Actual bytes has been write on actual file blocks: " + eosTotalBytes);
        }
    }

    public boolean isClosed() {
        return isClose;
    }

    public long getCurrentStart() {
        return currentStart;
    }

    public long getFinalStop() {
        return finalStop;
    }

    private CacheBlock newBlock(long blockStart, long blockStop) throws IOException {
        if (logger.isDebugEnabled())
            logger.info("Creating new block Starting With (including) " + blockStart + " - Ending with (Including) " + blockStop);
        if (type == MBType.DISK)
            return new FileBlock(blockStart, blockStop, settings.getMainFolder(), settings.getStateFolder(), cf, stats);
        else return new MemoryBlock(blockStart, blockStop, settings.getStateFolder(), pool, cf, stats);
    }

    private boolean findValidStartStop() {
        synchronized (reservedRanges) {
            MultiBlockUnifier rangeUnifier = new MultiBlockUnifier(reservedRanges);
            BlockRange startRange = rangeUnifier.findBlockContainsRange(currentStart);
            BlockRange emptySpace = rangeUnifier.findEmptySpace(startRange, currentStart, finalStop);

            if (startRange != null) {

                BlockRange remainingBufferIndex = new BlockRange(currentStart, startRange.getStop());
                if (remainingBufferIndex.contains(currentStart + bufferAvailable - 1)) {
                    currentStart += bufferAvailable;
                    return false;
                } else {
                    long skipLength = startRange.getStop() - currentStart + 1;
                    bufferAvailable -= skipLength;    //assume that already cached byte has been read
                    buffOffset += skipLength;        //skipping bytes that are already cached in another partial file
                }
                currentStart = emptySpace.getStart();
            }

            if (finalStop < emptySpace.getStop()) {
                currentStop = finalStop;
            } else {
                currentStop = emptySpace.getStop();
            }

            currentLength = currentStop - currentStart + 1;
            reservedRanges.add(new BlockRange(currentStart, currentStop));            //edo pu imaste synchronized
        }
        return true;
    }
}
