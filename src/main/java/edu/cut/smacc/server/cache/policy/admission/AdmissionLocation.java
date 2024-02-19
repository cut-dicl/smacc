package edu.cut.smacc.server.cache.policy.admission;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.common.StoreSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;


/**
 * This class helps with the enabled location to cache files.
 */
public abstract class AdmissionLocation implements AdmissionPolicy {

    private static final Logger logger = LogManager.getLogger(AdmissionLocation.class);

    protected StoreOptionType readLocation;
    protected StoreOptionType writeLocation;

    protected StoreSettings memorySettings;
    protected HashMap<Integer, StoreSettings> diskSettings;

    @Override
    public void initialize(Configuration conf) {
        /* Upload */
        String wLocation = conf.getString(ServerConfigurations.UPLOAD_LOCATION_KEY,
                ServerConfigurations.UPLOAD_LOCATION_DEFAULT);
        switch (wLocation) {
            case "S3ONLY" -> writeLocation = StoreOptionType.S3_ONLY;
            case "DISKONLY" -> writeLocation = StoreOptionType.DISK_ONLY;
            case "MEMORYONLY" -> writeLocation = StoreOptionType.MEMORY_ONLY;
            case "MEMORYDISK" -> writeLocation = StoreOptionType.MEMORY_DISK;
            default -> logger.error("Error setting ChooseTiersOnPut value (unknown value)");
        }

        /* Request */
        String rLocation = conf.getString(ServerConfigurations.REQUEST_LOCATION_KEY,
                ServerConfigurations.REQUEST_LOCATION_DEFAULT);
        switch (rLocation) {
            case "S3ONLY" -> readLocation = StoreOptionType.S3_ONLY;
            case "DISKONLY" -> readLocation = StoreOptionType.DISK_ONLY;
            case "MEMORYONLY" -> readLocation = StoreOptionType.MEMORY_ONLY;
            case "MEMORYDISK" -> readLocation = StoreOptionType.MEMORY_DISK;
            default -> logger.error("Error setting ChooseTiersOnGet value (unknown value)");
        }
        this.memorySettings = ServerConfigurations.getServerMemorySettigs();
        this.diskSettings = ServerConfigurations.getServerDiskVolumes();
    }

}
