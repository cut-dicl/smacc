package edu.cut.smacc.server.cache.common.io;

import edu.cut.smacc.server.cache.common.CacheOutOfMemoryException;
import edu.cut.smacc.server.cache.common.CacheType;
import edu.cut.smacc.server.cache.common.OutOfMemoryTierType;
import edu.cut.smacc.server.cloud.CloudFileWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class IsolateConcatOutputStream {
    private static final Logger logger = LogManager.getLogger(IsolateConcatOutputStream.class);


    private MultiBlockOutputStream outOne = null;
    private MultiBlockOutputStream outTwo = null;
    private CloudFileWriter cloudWriter = null;
    private ArrayList<MultiBlockOutputStream> outOfMemStreams = null;

    public IsolateConcatOutputStream(CloudFileWriter cloudWriter) {
        this.cloudWriter = cloudWriter;
    }

    public IsolateConcatOutputStream(MultiBlockOutputStream outOne, MultiBlockOutputStream outTwo,
                                     ArrayList<MultiBlockOutputStream> outOfMemStreams) {
        this.outOne = outOne;
        this.outTwo = outTwo;
        this.outOfMemStreams = outOfMemStreams;
    }

    public IsolateConcatOutputStream(MultiBlockOutputStream outOne, MultiBlockOutputStream outTwo,
            CloudFileWriter cloudWriter, ArrayList<MultiBlockOutputStream> outOfMemStreams) {
        this.outOne = outOne;
        this.outTwo = outTwo;
        this.cloudWriter = cloudWriter;
        this.outOfMemStreams = outOfMemStreams;
    }

    public IsolateConcatOutputStream(MultiBlockOutputStream outOne, ArrayList<MultiBlockOutputStream> outOfMemStreams) {
        this.outOne = outOne;
        this.outOfMemStreams = outOfMemStreams;
    }

    public IsolateConcatOutputStream(MultiBlockOutputStream outOne,
            CloudFileWriter cloudWriter, ArrayList<MultiBlockOutputStream> outOfMemStreams) {
        this.outOne = outOne;
        this.cloudWriter = cloudWriter;
        this.outOfMemStreams = outOfMemStreams;
    }

    public OutOfMemoryTierType write(int c) throws IOException {
        OutOfMemoryTierType evictionReturn = OutOfMemoryTierType.NONE;
        if (outOne != null)
            try {
                outOne.write(c);
            } catch (CacheOutOfMemoryException e) {
                outOfMemStreams.add(outOne);
                if (outOne.getCacheFile().type() == CacheType.DISK_FILE) evictionReturn = OutOfMemoryTierType.DISK;
                else evictionReturn = OutOfMemoryTierType.MEMORY;
                if (logger.isDebugEnabled()) logger.info("Cache Maximum Capacity Reached!");
            }
        if (outTwo != null)
            try {
                outTwo.write(c);
            } catch (CacheOutOfMemoryException e) {
                outOfMemStreams.add(outTwo);
                if (evictionReturn != OutOfMemoryTierType.NONE) evictionReturn = OutOfMemoryTierType.MEMORYDISK;
                else if (outTwo.getCacheFile().type() == CacheType.DISK_FILE) evictionReturn = OutOfMemoryTierType.DISK;
                else evictionReturn = OutOfMemoryTierType.MEMORY;
                if (logger.isDebugEnabled()) logger.info("Cache Maximum Capacity Reached!");
            }
        if (cloudWriter != null)
            cloudWriter.write(c);
        return evictionReturn;
    }

    public OutOfMemoryTierType write(byte[] buff) throws IOException {
        OutOfMemoryTierType evictionReturn = OutOfMemoryTierType.NONE;
        if (outOne != null)
            try {
                outOne.write(buff);
            } catch (CacheOutOfMemoryException e) {
                outOfMemStreams.add(outOne);
                if (outOne.getCacheFile().type() == CacheType.DISK_FILE) evictionReturn = OutOfMemoryTierType.DISK;
                else evictionReturn = OutOfMemoryTierType.MEMORY;
                if (logger.isDebugEnabled()) logger.info("Cache Maximum Capacity Reached!");
            }
        if (outTwo != null)
            try {
                outTwo.write(buff);
            } catch (CacheOutOfMemoryException e) {
                outOfMemStreams.add(outTwo);
                if (evictionReturn != OutOfMemoryTierType.NONE) evictionReturn = OutOfMemoryTierType.MEMORYDISK;
                else if (outTwo.getCacheFile().type() == CacheType.DISK_FILE) evictionReturn = OutOfMemoryTierType.DISK;
                else evictionReturn = OutOfMemoryTierType.MEMORY;
                if (logger.isDebugEnabled()) logger.info("Cache Maximum Capacity Reached!");
            }
        if (cloudWriter != null)
            cloudWriter.write(buff);
        return evictionReturn;
    }

    public OutOfMemoryTierType write(byte[] buff, int offset, int len) throws IOException {
        OutOfMemoryTierType evictionReturn = OutOfMemoryTierType.NONE;
        if (outOne != null)
            try {
                outOne.write(buff, offset, len);
            } catch (CacheOutOfMemoryException e) {
                outOfMemStreams.add(outOne);
                if (outOne.getCacheFile().type() == CacheType.DISK_FILE) evictionReturn = OutOfMemoryTierType.DISK;
                else evictionReturn = OutOfMemoryTierType.MEMORY;
                if (logger.isDebugEnabled()) logger.info("Cache Maximum Capacity Reached!");
            }
        if (outTwo != null)
            try {
                outTwo.write(buff, offset, len);
            } catch (CacheOutOfMemoryException e) {
                outOfMemStreams.add(outTwo);
                if (evictionReturn != OutOfMemoryTierType.NONE) evictionReturn = OutOfMemoryTierType.MEMORYDISK;
                else if (outTwo.getCacheFile().type() == CacheType.DISK_FILE) evictionReturn = OutOfMemoryTierType.DISK;
                else evictionReturn = OutOfMemoryTierType.MEMORY;
                if (logger.isDebugEnabled()) logger.info("Cache Maximum Capacity Reached!");
            }

        if (cloudWriter != null)
            cloudWriter.write(buff, offset, len);
        return evictionReturn;
    }

    public boolean hasS3Stream() {
        return cloudWriter != null;
    }

    public void abort() {
        if (outOne != null) outOne.abort();
        if (outTwo != null) outTwo.abort();
        if (cloudWriter != null)
            cloudWriter.abort();
    }

    public void flush() throws IOException {
        if (cloudWriter != null)
            cloudWriter.flush();
    }

    public void close() throws IOException {
        if (outOne != null) outOne.close();
        if (outTwo != null) outTwo.close();
        if (cloudWriter != null)
            cloudWriter.close();
    }

    public void detachStream(MultiBlockOutputStream stream) {
        if (outOne == stream) {
            outOne = null;
        } else if (outTwo == stream) {
            outTwo = null;
        } else logger.error("Cannot detach stream");
    }

    public void addStream(MultiBlockOutputStream stream) {
        if (outOne == null) outOne = stream;
        else if (outTwo == null) outTwo = stream;
        else logger.error("Cannot add stream");
    }

}