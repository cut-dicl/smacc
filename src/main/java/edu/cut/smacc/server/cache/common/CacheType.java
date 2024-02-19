package edu.cut.smacc.server.cache.common;

import java.io.IOException;

public enum CacheType {
    MEMORY_FILE(0), DISK_FILE(1);

    private final int cacheType;

    public int getInt() {
        return cacheType;
    }

    CacheType(int cacheType) {
        this.cacheType = cacheType;
    }

    public static CacheType getCacheType(int cacheType) throws IOException {
        return switch (cacheType) {
            case 0 -> MEMORY_FILE;
            case 1 -> DISK_FILE;
            default -> throw new IOException("Bad enum number...");
        };
    }
}