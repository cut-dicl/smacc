package edu.cut.smacc.server.cache.disk;

import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.CacheManagerBase;
import edu.cut.smacc.server.cache.common.FilenameFeatureExtractor;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.common.StoreSettings;
import edu.cut.smacc.server.cache.policy.CachePolicyNotifier;
import edu.cut.smacc.server.cache.policy.selection.DiskSelectionPolicy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * The disk manager controls the DiskFiles
 *
 * @author Theodoros Danos
 */
public class DiskManager extends CacheManagerBase {  // o disk manager tha vazi to file ston uploader
    private static final Logger logger = LogManager.getLogger(DiskManager.class);

    /* ******************* INSTANCE ******************* */

    private final HashMap<Integer, StoreSettings> diskSettings; // <MountPoint, Capacity>
    private final DiskSelectionPolicy diskSelectionPolicy;
    private final boolean isActive;

    public DiskManager(HashMap<Integer, StoreSettings> diskSettings, Configuration configuration,
                       CachePolicyNotifier policyNotifier) {
        setPolicyNotifier(policyNotifier);
        this.diskSettings = diskSettings;
        this.diskSelectionPolicy = DiskSelectionPolicy.getInstance(configuration);
        isActive = diskSettings != null && !diskSettings.isEmpty();
    }

    @Override
    public boolean isActive() {
        return isActive;
    }

    /* Interface */

    @Override
    public DiskFile create(String bucket, String key) throws IOException {
        DiskFile diskfile;
        StoreSettings selectedSettings = null;
        synchronized (cacheMapping) {
            touchBucket(bucket); // if bucket does not exist create a record

            Set<Integer> settingsIndices = diskSettings.keySet();
            int selectedDiskIndex = diskSelectionPolicy.getDiskIndex(diskSettings);
            for (Integer i : settingsIndices) {
                if (i == selectedDiskIndex)
                    selectedSettings = diskSettings.get(i);
            }
            if (selectedSettings == null) throw new IOException("Disk Index not found");
            diskfile = new DiskFile(bucket, key, selectedSettings, selectedDiskIndex, this);
        }
        return diskfile;
    }

    @Override
    public InputStream read(String bucket, String key) throws IOException {
        synchronized (this.cacheMapping) {
            try {
                if (fileDoesNotExist(bucket, key) || fileIsPartial(bucket, key)) {
                    return null;
                }
                CacheFile cacheFile = this.cacheMapping.get(bucket).get(key);
                policyNotifier.notifyItemAccess(cacheFile, getStoreOptionType());
                return cacheFile.getInputStream();
            } catch (FileNotFoundException e) {
                logger.error(e.getMessage());
                return null;
            }
        }
    }

    @Override
    public InputStream read(String bucket, String key, long start, long stop) throws IOException {
        synchronized (this.cacheMapping) {
            try {
                if (fileDoesNotExist(bucket, key)) {
                    return null;
                }
                CacheFile cacheFile = this.cacheMapping.get(bucket).get(key);
                policyNotifier.notifyItemAccess(cacheFile, getStoreOptionType());
                return ((DiskFile) cacheFile).getInputStream(start, stop);
            } catch (FileNotFoundException e) {
                logger.error(e.getMessage());
                return null;
            }
        }
    }

    @Override
    public StoreOptionType getStoreOptionType() {
        return StoreOptionType.DISK_ONLY;
    }

    private int getNumberOfDisks() {
        return diskSettings.size();
    }

    @Override
    public ArrayList<CacheFile> getCacheFiles() {
        ArrayList<CacheFile> files = new ArrayList<>((int) getCacheFilesCount());
        for (int i = 0; i < getNumberOfDisks(); i++) {
            files.addAll(getDiskCacheFiles(i));
        }
        return files;
    }

    @Override
    public long getCacheFilesCount() {
        long count = 0;
        synchronized (cacheMapping) {
            for (String bucket : cacheMapping.keySet())
                count += cacheMapping.get(bucket).size();
        }
        return count;
    }

    public ArrayList<CacheFile> getDiskCacheFiles(Integer integer) {
        ArrayList<CacheFile> files = new ArrayList<>();
        synchronized (cacheMapping) {
            for (String bucket : cacheMapping.keySet())
                for (String key : cacheMapping.get(bucket).keySet()) {
                    DiskFile file = (DiskFile) cacheMapping.get(bucket).get(key);
                    if (file.getDiskNumber() == integer)
                        files.add(file);
                }
        }
        return files;
    }

    @Override
    public long getReportedUsage() {
        long total = 0;
        for (int i = 0 ; i < getNumberOfDisks(); i++) {
            total += getDiskReportedUsage(i);
        }
        return total;
    }

    public long getDiskReportedUsage(Integer integer) {
        return diskSettings.get(integer).getStats().getReportedUsage();
    }

    private void touchBucket(String bucket) {
        synchronized (cacheMapping) {
            if (!cacheMapping.containsKey(bucket))
                cacheMapping.put(bucket, new HashMap<>()); // create bucket record
        }
    }

    public boolean isComplete(String bucket, String key) throws FileNotFoundException {
        if (!containsObject(bucket, key)) throw new FileNotFoundException("File not found!");
        return cacheMapping.get(bucket).get(key).isComplete();
    }

    public HashMap<String, DiskFile> initiateRecovery() {
        HashMap<String, DiskFile> returnLists = new HashMap<>();
        if (diskSettings == null)
            return returnLists;    // do not initialize since there are no configurations for disks

        //Disk Recovery
        StoreSettings settings;
        try {
            Set<Integer> settingsIndeces = diskSettings.keySet();
            for (Integer diski : settingsIndeces) {
                settings = diskSettings.get(diski);

                File statefolder = new File(settings.getStateFolder());
                File mainfolder = new File(settings.getMainFolder());

                File[] mainlistOfFiles = mainfolder.listFiles();

                if (mainlistOfFiles != null) {
                    //	Delete incomplete files
                    for (File file : mainlistOfFiles) {
                        if (file.isFile()) {
                            if (!file.getName().contains("##")) {
                                if (logger.isDebugEnabled())
                                    logger.info("Delete Incomplete DiskFile " + file.getName());
                                file.delete();
                                //	Incomplete files does not have a state file
                            }
                        }
                    }
                }


                mainlistOfFiles = mainfolder.listFiles();
                File[] statelistOfFiles = statefolder.listFiles();
                File mainFile;
                FilenameFeatureExtractor mainFileFeatures;

                if (mainlistOfFiles != null && statelistOfFiles != null) {
                    //	Recover complete or pushed files and delete obsolete files
                    for (File statelistOfFile : statelistOfFiles) {
                        if (statelistOfFile.isFile()) {
                            if (statelistOfFile.getName().contains("##")) {
                                String stateFilename = statelistOfFile.getName();
                                FilenameFeatureExtractor features = new FilenameFeatureExtractor(stateFilename);
                                if (features.isCacheFile()) {
                                    //	Find main file
                                    mainFile = null;

                                    for (File file : mainlistOfFiles)
                                        if (file.isFile()) {
                                            mainFileFeatures = new FilenameFeatureExtractor(file.getName());
                                            if (mainFileFeatures.equals(
                                                    new FilenameFeatureExtractor(statelistOfFile.getName()), true)
                                                    &&
                                                    mainFileFeatures.getRange().getLength() == file.length()) {
                                                mainFile = file;
                                                break;
                                            } else if (mainFileFeatures.isCacheFile() && mainFileFeatures.getRange().getLength() != file.length())
                                                file.delete();
                                        }

                                    //	Delete file if obsolete (check must be done after finding the main file)
                                    if (features.isObsolete()) {
                                        if (logger.isDebugEnabled())
                                            logger.info("Delete Obsolete DiskFile " + statelistOfFile.getName());
                                        statelistOfFile.delete();
                                        if (mainFile != null) mainFile.delete();
                                        continue;
                                    }

                                    //	If complete or pushed, make the file visible
                                    if (mainFile != null && mainFile.exists())        // If there is a main file in disk - and not only the state file - and is not deleted (because it was obsolete)
                                    {
                                        if (!returnLists.containsKey(features.getBucket() + features.getKey())) {
                                            DiskFile recoveredFile = new DiskFile(diskSettings.get(diski), diski, this, features);
                                            recoveredFile.recoverBlock(mainFile);
                                            returnLists.put(features.getBucket() + features.getKey(), recoveredFile);
                                            put(features.getBucket(), features.getKey(), recoveredFile);
                                        } else
                                            returnLists.get(features.getBucket() + features.getKey()).recoverBlock(mainFile);

                                    } else {
                                        if (logger.isDebugEnabled()) logger.info("Recovery[D]: No main file found - skip");
                                        // We have only the state file not actual file, so delete the state file
                                        statelistOfFile.delete();
                                    }
                                }

                            }
                        }
                    }
                }

            }
            return returnLists;
        } catch (Exception e) {
            logger.fatal("Initial Recovery: " + e.getMessage(), e);
            e.printStackTrace();
            return null;
        }
    }

    public void shutdown() {
    }
}