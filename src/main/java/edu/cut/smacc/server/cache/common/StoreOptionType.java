package edu.cut.smacc.server.cache.common;

import java.io.IOException;

/**
 * @author Michail Boronikolas
 */
public enum StoreOptionType {
    MEMORY_ONLY(0), DISK_ONLY(1), MEMORY_DISK(2), S3_ONLY(3);

    private final int storeOptionType;

    public int getInt() {
        return storeOptionType;
    }

    StoreOptionType(int storeOptionType) {
        this.storeOptionType = storeOptionType;
    }

    public static StoreOptionType getStoreOptionType(int cacheType) throws IOException {
        return switch (cacheType) {
            case 0 -> MEMORY_ONLY;
            case 1 -> DISK_ONLY;
            case 2 -> MEMORY_DISK;
            case 3 -> S3_ONLY;
            default -> throw new IOException("Bad enum number...");
        };
    }
}
