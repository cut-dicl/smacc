package edu.cut.smacc.server.s3;

import edu.cut.smacc.server.cache.common.BlockRange;
import edu.cut.smacc.server.cache.common.CacheBlock;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.CacheType;
import edu.cut.smacc.server.cache.common.StateType;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.server.cache.common.io.MultiBlockOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Represents an S3File that will not be cached
 */
public class S3File implements CacheFile {

    private String bucket;
    private String key;
    private long actualSize;
    private long lastS3Modification;

    public S3File(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return null;
    }

    @Override
    public MultiBlockOutputStream getOutputStream() throws IOException {
        return null;
    }

    @Override
    public MultiBlockOutputStream getOutputStream(long start, long stop) throws IOException {
        return null;
    }

    @Override
    public void delete() {

    }

    @Override
    public StoreOptionType getStoreOption() {
        return StoreOptionType.S3_ONLY;
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

    @Override
    public InputStream getIncompleteInputStream() throws IOException {
        return null;
    }

    @Override
    public void addIncompleteBlock(CacheBlock block) {
    }

    @Override
    public void makeBlockVisible(CacheBlock block) {
    }

    @Override
    public void setActualSize(long size) {
        this.actualSize = size;
    }

    @Override
    public boolean isFullFile() {
        return true;
    }

    @Override
    public boolean inRange(long start, long stop) {
        return start >= 0 && stop <= actualSize;
    }

    @Override
    public void decReadQueue(InputStream in) throws IOException {
    }

    @Override
    public String getBucket() {
        return bucket;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void stateComplete() {
    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public void setVersion(long version) {
    }

    @Override
    public void stateToBePushed() {
    }

    @Override
    public CacheType type() {
        return null;
    }

    @Override
    public void deleteIfIncomplete() {
    }

    @Override
    public boolean isPushed() {
        return false;
    }

    @Override
    public boolean isComplete() {
        return true;
    }

    @Override
    public boolean isObsolete() {
        return false;
    }

    @Override
    public StoreSettings getSettings() {
        return null;
    }

    @Override
    public List<BlockRange> getReservedRanges() {
        return null;
    }

    @Override
    public boolean isPartialFile() {
        return false;
    }

    @Override
    public void setLastModified(long lastModified) {
        this.lastS3Modification = lastModified;
    }

    @Override
    public long getLastModified() {
        return lastS3Modification;
    }

    @Override
    public void replaceReservedRange(BlockRange prevRange, BlockRange newRange) {
    }

    @Override
    public void setPartialFile() {
    }

    @Override
    public long getLastTimeUsed() {
        return lastS3Modification;
    }

    @Override
    public long getSize() {
        return actualSize;
    }

    @Override
    public long getActualSize() {
        return actualSize;
    }

    @Override
    public List<CacheBlock> getCacheBlocks() {
        return null;
    }

    @Override
    public void lockReader() {
    }

    @Override
    public long getTotalSize() {
        return actualSize;
    }

    @Override
    public void abortWrite() {
    }

    @Override
    public StateType getState() {
        return StateType.COMPLETE;
    }

    @Override
    public List<BlockRange> getBlockRangeList() {
        return null;
    }
}
