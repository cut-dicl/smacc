package edu.cut.smacc.server.cache.policy.admission;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;

public class AdmissionAlways extends AdmissionLocation {

    @Override
    public StoreOptionType getReadAdmissionLocation(CacheFile file) {
        return readLocation;
    }

    @Override
    public StoreOptionType getWriteAdmissionLocation(CacheFile file) {
        return writeLocation;
    }


    @Override
    public void onItemAdd(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemNotAdded(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemAccess(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemUpdate(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void onItemDelete(CacheFile file, StoreOptionType tier) {
        // Do nothing
    }

    @Override
    public void reset() {
        // Do nothing
    }

}
