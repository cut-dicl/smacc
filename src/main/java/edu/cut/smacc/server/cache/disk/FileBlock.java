package edu.cut.smacc.server.cache.disk;

import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.common.io.UsageStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * handles a disk block file
 *
 * @author Theodoros Danos
 */
public class FileBlock implements CacheBlock {
    /* Static */
    private static final Logger logger = LogManager.getLogger(FileBlock.class);

    /* Instance */
    private String mainFolder;
    private String stateFolder;
    private BlockRange range;
    private String bucket;
    private String key;
    private File blockFile;
    private File blockStateFile;
    private long version;
    private StateType state;
    private long writeSoFar = 0;
    private FileOutputStream outputStream;
    private boolean isDeleted = false;
    private UsageStats parentStats;
    private CacheFile cf;
    private boolean isClosed = false;

    public FileBlock(long start, long stop, String mainFolder, String stateFolder, CacheFile cf, UsageStats parentStats) throws IOException {
        range = new BlockRange(start, stop);
        this.mainFolder = mainFolder;
        this.stateFolder = stateFolder;
        this.bucket = cf.getBucket();
        this.key = cf.getKey();
        this.version = cf.getVersion();
        this.cf = cf;
        this.parentStats = parentStats;
        state = StateType.INCOMPLETE;
        setTemporaryFilename();
    }

    // 	Constructor is used in recovery only
    FileBlock(FilenameFeatureExtractor features, StateType state, StoreSettings settings, CacheFile cf) {
        this.range = features.getRange();
        this.mainFolder = settings.getMainFolder();
        this.stateFolder = settings.getStateFolder();
        this.bucket = cf.getBucket();
        this.key = cf.getKey();
        this.version = cf.getVersion();
        this.cf = cf;
        this.parentStats = settings.getStats();
        this.state = state;
        this.blockFile = new File(settings.getMainFolder() + features.getFilename());
        blockStateFile = new File(generateStateFilename(state));
        settings.getStats().increment(features.getRange().getLength());

        if (logger.isDebugEnabled())
            logger.info("File Restored [" + mainFolder + key + "]: " + range.toString() + " " + cf + " " + this);
    }

    private void setTemporaryFilename() throws IOException {
        Random rn = new Random();
        String filename = generateTempFilename(rn);
        File tempFile = new File(filename);
        while (tempFile.exists()) {
            filename = generateTempFilename(rn);
            tempFile = new File(filename);
        }
        tempFile.createNewFile();
        this.blockFile = tempFile;
        outputStream = new FileOutputStream(blockFile);
    }

    private String generateStateFilename(StateType setState) {
        StringBuilder sb = new StringBuilder();

        sb.append(stateFolder);
        switch (setState) {
            case COMPLETE -> sb.append("COMPLETE$");
            case INCOMPLETE -> sb.append("INCOMPLETE$");
            case OBSOLETE -> sb.append("OBSOLETE$");
            case TOBEPUSHED -> sb.append("PUSHED$");
            default -> throw new IllegalArgumentException("Unexpected value: " + setState);
        }

        sb.append(version);
        sb.append("##");
        sb.append(StringShort.toHex(bucket));
        sb.append("#");
        sb.append(StringShort.toHex(key));
        sb.append("-");
        sb.append(range.getStart());
        sb.append("-");
        sb.append(range.getStop());

        return sb.toString();
    }

    private String generateMainFilename() {
        StringBuilder sb = new StringBuilder();

        sb.append(mainFolder);
        sb.append(version);
        sb.append("##");
        sb.append(StringShort.toHex(bucket));
        sb.append("#");
        sb.append(StringShort.toHex(key));
        sb.append("-");
        sb.append(range.getStart());
        sb.append("-");
        sb.append(range.getStop());

        return sb.toString();
    }

    private String generateTempFilename(Random rn) {
        StringBuilder sb = new StringBuilder();

        sb.append(mainFolder);
        sb.append(StringShort.toHex(bucket));
        sb.append("#");
        sb.append(StringShort.toHex(key));
        sb.append("@");
        sb.append(rn.nextInt() & Integer.MAX_VALUE);

        return sb.toString();
    }

    public void setStateFileObsolete() {
        StateType prevState = state;
        if (prevState != StateType.INCOMPLETE) {
            String previousFile = blockStateFile.getAbsolutePath();
            state = StateType.OBSOLETE;
            String newFile = generateStateFilename(StateType.OBSOLETE);

            try {
                Files.move(Paths.get(previousFile), Paths.get(newFile), REPLACE_EXISTING);
            } catch (IOException e) {
                logger.error("Could not rename state file: " + e.getMessage());
            }//LOG EVENT and continue
            blockStateFile = new File(newFile);
        }
    }

    public void abortWrite() {
        try {
            outputStream.close();
        } catch (IOException ignored) {
        }
        uncheckedDelete();
    }

    private void deleteStateFile() {
        if (!isIncomplete()) {
            if (blockStateFile != null && blockStateFile.exists())
                blockStateFile.delete();    //delete stateFile if not incomplete (there are no incomplete state files)
        }
    }

    public void complete() {
        try {
            if (logger.isDebugEnabled()) logger.info("File Block Complete");

            String stateFilename;

            //CREATE/RENAME STATE FILE
            if (state == StateType.INCOMPLETE)    //if incomplete - then create complete state file
            {
                stateFilename = generateStateFilename(StateType.COMPLETE);
                blockStateFile = new File(stateFilename);
                blockStateFile.createNewFile();
            } else if (state == StateType.TOBEPUSHED) {    //if from pushed changed to complete, then rename the file
                File newStateFile = new File(generateStateFilename(StateType.COMPLETE));
                Files.move(Paths.get(blockStateFile.getAbsolutePath()), Paths.get(newStateFile.getAbsolutePath()), REPLACE_EXISTING);
                blockStateFile = newStateFile;
            }

            //FROM INCOMPLETE (main file) - MAKE IT COMPLETE

            if (state == StateType.INCOMPLETE) {
                String newFile = generateMainFilename();
                if (cf.isPartialFile()) newFile += ".partial";
                String previousFile = blockFile.getAbsolutePath();
                Files.move(Paths.get(previousFile), Paths.get(newFile), REPLACE_EXISTING);
                blockFile = new File(newFile);
            }

            this.state = StateType.COMPLETE;

        } catch (Exception e) {
            logger.error("Could not complete disk block. Error:" + e.getMessage());
        }
    }

    public void toBePushed() {
        try {
            //CREATE STATE FILE
            String stateFile = generateStateFilename(StateType.TOBEPUSHED);
            blockStateFile = new File(stateFile);
            blockStateFile.createNewFile();

            //RENAME MAIN FILE: FROM INCOMPLETE - PUSH IT TO ASYNC
            //filename = path/version##hex(bucket/key)-start-stop
            String newFile = generateMainFilename();
            String previousFile = blockFile.getAbsolutePath();
            blockFile = new File(newFile);
            Files.move(Paths.get(previousFile), Paths.get(newFile), REPLACE_EXISTING);

            this.state = StateType.TOBEPUSHED;

        } catch (IOException e) {
            logger.error("Disk Pushed Error: " + e.getMessage());
        }
    }

    public InputStream getFileInputStream() throws IOException {
        return new FileInputStream(blockFile);
    }

    public long getSize() {
        return range.getLength();
    }

    public void write(byte[] buffer, int offset, int len) throws IOException {
        outputStream.write(buffer, offset, len);
        writeSoFar += len;
    }

    public void write(int c) throws IOException {
        outputStream.write(c);
        writeSoFar += 1;
    }

    public long available() {
        return range.getLength() - writeSoFar;
    }

    public boolean close() throws IOException {
        boolean isObs;
        if (range.getStop() == -1) {
            range.update(range.getStart(), writeSoFar - 1);
        }

        synchronized (this) {
            outputStream.close();
            isClosed = true;
            isObs = isObsolete();
        }

        if (isObs) {
            if (logger.isDebugEnabled()) logger.info("File Block Obsolete - Delete");
            blockFile.delete();
            return false;
        } else if (available() == 0)
            return true;
        else {
            BlockRange newRange = new BlockRange(range.getStart(), range.getStart() + writeSoFar - 1);
            BlockRange prevRange = range;
            cf.replaceReservedRange(prevRange, newRange);
            range = newRange;
            if (logger.isDebugEnabled())
                logger.info("Premature Closing Block - Available: " + available() + " New Range: " + newRange.toString());
            return true;
        }
    }

    public void delete() {
        if (!isDeleted) {
            if (isIncomplete()) {
                synchronized (this) {
                    if (!isClosed)
                        this.state = StateType.OBSOLETE;
                    else
                        uncheckedDelete();
                }
            } else
                uncheckedDelete();
        }
    }

    private void uncheckedDelete() {
        if (logger.isDebugEnabled()) logger.info("DELETE: File block");
        isDeleted = true;
        blockFile.delete();
        deleteStateFile();
        parentStats.decrement(writeSoFar, writeSoFar);
    }

    public BlockRange getRange() {
        return range;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public boolean isComplete() {
        return state == StateType.COMPLETE;
    }

    public boolean isPushed() {
        return state == StateType.TOBEPUSHED;
    }

    public boolean isIncomplete() {
        return state == StateType.INCOMPLETE;
    }

    public boolean isObsolete() {
        return state == StateType.OBSOLETE;
    }

    public StateType getState() {
        return state;
    }
}









