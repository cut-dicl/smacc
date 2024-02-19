package edu.cut.smacc.server.cache.common.io;

import edu.cut.smacc.server.cache.common.BlockRange;
import edu.cut.smacc.server.cache.common.CacheBlock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This class helps in manage multiple files (blocks) as one virtual file
 *
 * @author Theodoros Danos
 */

@SuppressWarnings({"unused", "ResultOfMethodCallIgnored"})

public class MultiBlockInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(MultiBlockInputStream.class);


    private long currentStop;
    private LinkedList<CacheBlock> readBlockList;
    private CacheBlock currentBlock;
    private InputStream internalIn;
    private long maxBlockLength;
    private long readSoFar;

    public MultiBlockInputStream(long start, long stop, List<CacheBlock> fileBlocks) throws IOException {
        long tempStart = start;
        readBlockList = new LinkedList<>();
        List<CacheBlock> blocksInRange = new ArrayList<>();
        CacheBlock startingBlock = null;
        currentStop = stop;


        /*Find which blocks are in specified range*/
        for (CacheBlock block : fileBlocks) {
            BlockRange rangeStr = block.getRange();
            if (rangeStr.getStart() >= start && rangeStr.getStop() <= stop) //add blocks between first and last block
                blocksInRange.add(block);
            else if (rangeStr.contains(stop))  //add last block
                blocksInRange.add(block);

            if (rangeStr.contains(start)) {
                tempStart = block.getRange().getStop() + 1;
                readBlockList.addFirst(block);
            }
        }


        //add blocks in queue with correct order
        while (stop > tempStart) {
            Iterator<CacheBlock> iter = blocksInRange.iterator();
            while (iter.hasNext()) {
                CacheBlock block = iter.next();
                if (block.getRange().getStart() == tempStart) {
                    tempStart = block.getRange().getStop() + 1;
                    readBlockList.addFirst(block);
                    iter.remove();
                }
            }
            if (blocksInRange.isEmpty()) break;
        }
        blocksInRange.clear();

        currentBlock = readBlockList.pollLast();
        assert currentBlock != null;
        if (!currentBlock.getRange().contains(stop))    //if the first is not the last block
            maxBlockLength = currentBlock.getRange().getStop() - start + 1;
        else                                                //if the first is also the last block
            maxBlockLength = stop - start + 1;
        internalIn = currentBlock.getFileInputStream();
        internalIn.skip(start);
    }

    private void changeBlock() throws IOException {
        //changeBlock has to be called after the increment of readSoFar variable
        if (available() == 0) {
            internalIn.close();
            currentBlock = readBlockList.pollLast();

            if (currentBlock != null) {
                internalIn = currentBlock.getFileInputStream();
                if (readBlockList.isEmpty()) //last block
                    maxBlockLength = currentStop - currentBlock.getRange().getStart() + 1;
                else
                    maxBlockLength = currentBlock.getSize();

                readSoFar = 0;
            }
        }
    }

    public int read() throws IOException {
        try {
            if (maxBlockLength == readSoFar && readBlockList.isEmpty()) return -1;
            else {
                int r = internalIn.read();
                if (r > 0) readSoFar += r;
                changeBlock();
                return r;
            }
        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public int read(byte[] buffer) throws IOException {
        int r;
        try {
            if (maxBlockLength == readSoFar && readBlockList.isEmpty()) return -1;
            int bufferLength = buffer.length;
            int availableInFile = available();
            if (bufferLength > availableInFile) {
                r = internalIn.read(buffer, 0, availableInFile);
            } else {
                r = internalIn.read(buffer, 0, bufferLength);
            }

            if (r > 0) readSoFar += r;
            changeBlock();
            return r;

        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public int read(byte[] buffer, int offset, int len) throws IOException {
        int r;
        try {
            if (maxBlockLength == readSoFar && readBlockList.isEmpty()) return -1;
            int availableInFile = available();
            if (len > availableInFile) {
                r = internalIn.read(buffer, offset, availableInFile);
            } else {
                r = internalIn.read(buffer, offset, len);
            }
            if (r > 0) readSoFar += r;
            changeBlock();
            return r;

        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public void reset() throws IOException {
        try {
            internalIn.reset();
        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public long skip(long n) throws IOException {
        try {
            return internalIn.skip(n);
        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public void mark(int readLimit) {
        internalIn.mark(readLimit);
    }

    public boolean markSupported() {
        return internalIn.markSupported();
    }

    public int available() {
        long avail = maxBlockLength - readSoFar;
        if (avail > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        else return (int) avail;
    }

    public void close() throws IOException {
        internalIn.close();
    }
}
