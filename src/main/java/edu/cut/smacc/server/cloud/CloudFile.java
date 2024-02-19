package edu.cut.smacc.server.cloud;

import edu.cut.smacc.server.cache.common.StateType;

/**
 * Holds information about a file/object stored on a cloud storage
 */
public class CloudFile {

    private String bucket;
    private String key;
    private long length;
    private long version;
    private StateType state;
    private long lastModified;
    private boolean isOwnedFile;

    /**
     * Constructor for a new incomplete file
     * 
     * @param bucket
     * @param key
     * @param length
     */
    public CloudFile(String bucket, String key, long length) {
        this.bucket = bucket;
        this.key = key;
        this.length = length;
        this.version = 0;
        this.state = StateType.INCOMPLETE;
        this.lastModified = System.currentTimeMillis();
        this.isOwnedFile = true;
    }

    /**
     * Constructor for an existing complete file
     * 
     * @param bucket
     * @param key
     * @param length
     * @param lastModified
     * @param isOwnedFile
     */
    public CloudFile(String bucket, String key, long length, long lastModified, boolean isOwnedFile) {
        this.bucket = bucket;
        this.key = key;
        this.length = length;
        this.version = 0;
        this.state = StateType.COMPLETE;
        this.lastModified = lastModified;
        this.isOwnedFile = isOwnedFile;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public long getLength() {
        return length;
    }

    public long getVersion() {
        return version;
    }

    public boolean isIncomplete() {
        return state == StateType.INCOMPLETE;
    }

    public boolean isComplete() {
        return state == StateType.COMPLETE;
    }

    public boolean isObsolete() {
        return state == StateType.OBSOLETE;
    }

    public long getLastModified() {
        return lastModified;
    }

    public boolean isOwnedFile() {
        return isOwnedFile;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void setState(StateType state) {
        this.state = state;
    }

    public String toString() {
        return bucket + ":" + key;
    }

}
