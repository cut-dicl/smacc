package edu.cut.smacc.server.cache.common;

/**
 * Provides information of a smacc file, such as the version, key, state etc
 *
 * @author Theodoros Danos
 */
public class FilenameFeatureExtractor {

    /* INSTANCE */

    private StateType state = null;
    private long version;
    private String s3key;
    private String s3bucket;
    private boolean isCacheFile;
    private String filename;
    private BlockRange range;
    private boolean isPartial = false;

    public FilenameFeatureExtractor(String filename) {
        isCacheFile = false;

        this.filename = filename;
        String[] stateAndFilename = filename.split("\\$");

        if (stateAndFilename.length == 2) { // It is a state filename
            if (stateAndFilename[0].equals("COMPLETE") || stateAndFilename[0].equals("PUSHED") || stateAndFilename[0].equals("OBSOLETE")) {
                if (stateAndFilename[0].equals("COMPLETE")) state = StateType.COMPLETE;
                else if (stateAndFilename[0].equals("PUSHED")) state = StateType.TOBEPUSHED;
                else state = StateType.OBSOLETE;

                processFilename(stateAndFilename[1]);
            }
        } else // it is a filename of a data disk file
            processFilename(filename);
    }

    /**
     * Encoding:
     * <version>##<bucket>#<key>-<start_offset>-<end_offset>[.partial]
     * Example:
     * 1##736d6163636865726f#6865726f2f726561646d65-0-5493
     * 
     * @param fname
     */
    private void processFilename(String fname) {
        if (fname.endsWith(".partial")) {
            isPartial = true;
            fname = fname.substring(0, fname.length() - 8);
        }
        String[] versionAndFilename = fname.split("##");

        if (versionAndFilename.length == 2) {
            try {
                version = Long.parseLong(versionAndFilename[0]);
            } catch (NumberFormatException e) {
                return;
            }

            String[] filenameAndRange = versionAndFilename[1].split("-");
            if (filenameAndRange.length != 3)
                return;

            String encodedFilename = filenameAndRange[0];
            String[] encodedBucketKey = encodedFilename.split("#");
            if (encodedBucketKey.length != 2)
                return;

            s3bucket = StringShort.fromHex(encodedBucketKey[0]);
            s3key = StringShort.fromHex(encodedBucketKey[1]);

            try {
                range = new BlockRange(Long.parseLong(filenameAndRange[1]), Long.parseLong(filenameAndRange[2]));
            } catch (NumberFormatException e) {
                return;
            }

            if (version >= 0) isCacheFile = true;
        }
    }

    public boolean isPartial() {
        return isPartial;
    }

    public long getVersion() {
        return version;
    }

    public boolean isCacheFile() {
        return isCacheFile;
    }

    public String getKey() {
        return s3key;
    }

    public String getBucket() {
        return s3bucket;
    }

    public boolean isObsolete() {
        return state == StateType.OBSOLETE;
    }

    public boolean isComplete() {
        return state == StateType.COMPLETE;
    }

    public boolean isToBePushed() {
        return state == StateType.TOBEPUSHED;
    }

    public StateType getState() {
        return this.state;
    }

    public String getFilename() {
        return filename;
    }

    public BlockRange getRange() {
        return range;
    }

    public boolean containsRange(BlockRange compRange) {
        return compRange.contains(range.getStart()) && compRange.contains(range.getStop());
    }

    public boolean equals(FilenameFeatureExtractor file, boolean checkRange) {
        if (isCacheFile() || file.isCacheFile()) {
            if (getVersion() == file.getVersion()) {
                boolean crange = !checkRange || getRange().equals(file.getRange());
                return getBucket().equals(file.getBucket()) && getKey().equals(file.getKey()) && crange;
            }
        }
        return false;
    }
}
