package edu.cut.smacc.server.cache.common;

import edu.cut.smacc.server.cache.common.io.MultiBlockOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface CacheFile extends Comparable<CacheFile> {
    InputStream getInputStream() throws IOException;

    InputStream getIncompleteInputStream() throws IOException;

    MultiBlockOutputStream getOutputStream() throws IOException;

    MultiBlockOutputStream getOutputStream(long start, long stop) throws IOException;

    void addIncompleteBlock(CacheBlock block);

    void makeBlockVisible(CacheBlock block);

    void setActualSize(long size);

    boolean isFullFile();

    boolean inRange(long start, long stop);

    void decReadQueue(InputStream in) throws IOException;

    String getBucket();

    String getKey();

    void stateComplete();

    void delete();

    long getVersion();

    void setVersion(long version);

    void stateToBePushed();

    CacheType type();

    void deleteIfIncomplete();

    boolean isPushed();

    boolean isComplete();

    boolean isObsolete();

    StoreSettings getSettings();

    List<BlockRange> getReservedRanges();

    boolean isPartialFile();

    void setLastModified(long lastModified);

    long getLastModified();

    void replaceReservedRange(BlockRange prevRange, BlockRange newRange);

    void setPartialFile();

    long getLastTimeUsed();

    long getSize();

    long getActualSize();    //size of actual s3 file

    List<CacheBlock> getCacheBlocks();

    void lockReader();

    long getTotalSize();

    void abortWrite();

    StateType getState();

    List<BlockRange> getBlockRangeList();

    StoreOptionType getStoreOption();
}
