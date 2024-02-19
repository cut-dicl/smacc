package edu.cut.smacc.server.cache.common;

import edu.cut.smacc.server.cache.common.io.UsageStats;

/**
 * Store settings helps to keep information about a storage device (e.g. memory or a disk)
 *
 * @author Theodoros Danos
 */
public class StoreSettings {
    private String mainFolder;
    private String stateFolder;
    private UsageStats cacheStoreStats;

    public StoreSettings(String mainFolder, String stateFolder,
                         UsageStats cacheStoreStats) {
        if (mainFolder != null && !mainFolder.endsWith("/"))
            mainFolder += "/";
        if (!stateFolder.endsWith("/"))
            stateFolder += "/";

        this.mainFolder = mainFolder;
        this.stateFolder = stateFolder;
        this.cacheStoreStats = cacheStoreStats;
    }

    public String getStateFolder() {
        return stateFolder;
    }

    public String getMainFolder() {
        return mainFolder;
    }

    public UsageStats getStats() {
        return cacheStoreStats;
    }
}

