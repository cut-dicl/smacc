package edu.cut.smacc.server.cache.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Is used in order to construct information of an object being in cache. It is used by cacheList() method of Class SMACCClient.
 *
 * @author Theodoros Danos
 */

public class SMACCObject {

    private String key;
    private String bucket;
    private final long actualSize;
    private final long cacheSize;
    private final boolean isPartial;
    private final CacheType type;
    private StoreOptionType storeOptionType;
    private StateType state;
    private final List<BlockRange> ranges;
    private long lastModified;

    public SMACCObject(String key, String bucket, long actualSize, long cacheSize, boolean isPartial, CacheType type,
            StoreOptionType storeOptionType, long lastModified, StateType state, List<BlockRange> ranges) {
        this.key = key;
        this.bucket = bucket;
        this.ranges = ranges;
        this.actualSize = actualSize;
        this.cacheSize = cacheSize;
        this.isPartial = isPartial;
        this.type = type;
        this.storeOptionType = storeOptionType;
        this.lastModified = lastModified;
        this.state = state;
    }

    public SMACCObject(CacheFile file) {
        key = file.getKey();
        bucket = file.getBucket();
        actualSize = file.getActualSize();
        cacheSize = file.getSize();            //takes into account only completed blocks
        isPartial = file.isPartialFile();
        type = file.type();
        state = file.getState();
        ranges = file.getBlockRangeList();
        if (file.getLastModified() != 0)
            lastModified = file.getLastModified();
        else
            lastModified = file.getLastTimeUsed();

        if (type == CacheType.MEMORY_FILE) {
            storeOptionType = StoreOptionType.MEMORY_ONLY;
        } else if (type == CacheType.DISK_FILE) {
            storeOptionType = StoreOptionType.DISK_ONLY;
        }
    }

    /* Getters */

    public String getKey() {
        return key;
    }

    public String getBucket() {
        return bucket;
    }

    public long getActualSize() {
        return actualSize;
    }

    public StateType getState() {
        return state;
    }

    public CacheType getType() {
        return type;
    }

    public boolean isPartialFile() {
        return isPartial;
    }

    public long getCacheObjectSize() {
        return cacheSize;
    }

    public List<BlockRange> getBlockRangeList() {
        return ranges;
    }

    public StoreOptionType getStoreOptionType() {
        return storeOptionType;
    }

    public long getLastModified() {
        return lastModified;
    }

    /* Setters */

    public void setKey(String key) {
        this.key = key;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public void setState(StateType state) {
        this.state = state;
    }

    public void send(DataOutputStream out) throws IOException {
        if (ranges != null) {
            out.writeInt(ranges.size());
            for (BlockRange range : ranges) {
                range.send(out);
            }
        } else {
            out.writeInt(0);
        }

        out.writeShort(key.length());
        out.write(key.getBytes());
        out.writeShort(bucket.length());
        out.write(bucket.getBytes());
        out.writeLong(actualSize);
        out.writeLong(cacheSize);
        out.writeBoolean(isPartial);
        out.write(type == null ? -1 : type.getInt());
        out.write(storeOptionType.getInt());
        out.writeLong(lastModified);
        out.write(state.getInt());
    }

    public void setStoreOptionType(StoreOptionType storeOptionType) {
        this.storeOptionType = storeOptionType;
    }

    public String getKeyAndStorageTier() {
        return getKey() + ", Storage tier: " + storeOptionType.toString();
    }

    public static SMACCObject receive(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<BlockRange> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(BlockRange.receive(in));
        }

        return new SMACCObject(
        		new String(in.readNBytes(in.readShort())),
                new String(in.readNBytes(in.readShort())),
                in.readLong(),
                in.readLong(),
                in.readBoolean(),
                CacheType.getCacheType(in.read()),
                StoreOptionType.getStoreOptionType(in.read()),
                in.readLong(),
                StateType.getStateType(in.read()),
                list);
    }

    @Override
    public String toString() {
        return "SMACCObject{" +
                "key='" + key + '\'' +
                ", bucket='" + bucket + '\'' +
                ", actualSize=" + actualSize +
                ", cacheSize=" + cacheSize +
                ", isPartial=" + isPartial +
                ", type=" + type +
                ", state=" + state +
                ", ranges=" + ranges +
                '}';
    }

    public String metadataString() {
        return "SMACC Object[" +
                "Key: " + key +
                ", Actual Size: " + actualSize +
                ", Cache Size: " + cacheSize +
                ", Last Modified: " + new Date(lastModified) +
                ", Tier: " + storeOptionType +
                ']';
    }
}
