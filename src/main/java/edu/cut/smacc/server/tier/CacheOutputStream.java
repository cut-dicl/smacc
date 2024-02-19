package edu.cut.smacc.server.tier;

import edu.cut.smacc.server.cache.CacheFileHelper;
import edu.cut.smacc.server.cache.common.*;
import edu.cut.smacc.server.cache.common.io.IsolateConcatOutputStream;
import edu.cut.smacc.server.cache.common.io.MultiBlockOutputStream;
import edu.cut.smacc.server.cache.disk.DiskManager;
import edu.cut.smacc.server.cache.policy.ContinueToDiskPolicy;
import edu.cut.smacc.server.cloud.AsyncCloudUploadManager;
import edu.cut.smacc.server.cloud.CloudFile;
import edu.cut.smacc.server.cloud.CloudFileReader;
import edu.cut.smacc.server.cloud.CloudFileWriter;
import edu.cut.smacc.configuration.ServerConfigurations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is the core of writing a cache file. the class understands the logic of smacc cache and the states of each file.
 * Is responsible to process the state of the files and connect the files with a stream of writing
 *
 * @author Theodoros Danos
 */
public class CacheOutputStream extends OutputStream {
    /* **************** STATIC ***************** */
    private static final Logger logger = LogManager.getLogger(CacheOutputStream.class);
    // bucket -> key -> lock
    private static final Map<String, Map<String, ReentrantLock>> stateLockMapping = Collections
            .synchronizedMap(new HashMap<>());

    private static void lock(String bucket, String key) {
        synchronized (stateLockMapping) {
            if (!stateLockMapping.containsKey(bucket)) {
                stateLockMapping.put(bucket, Collections.synchronizedMap(new HashMap<>()));
            }
        }

        Map<String, ReentrantLock> innerStateLockMapping = stateLockMapping.get(bucket);
        synchronized (innerStateLockMapping) {
            if (!innerStateLockMapping.containsKey(key)) {
                innerStateLockMapping.put(key, new ReentrantLock());
            }
        }

        innerStateLockMapping.get(key).lock();
    }

    private static void unlock(String bucket, String key) {
        stateLockMapping.get(bucket).get(key).unlock();
    }

    /* *************** INSTANCE ****************** */
    private ArrayList<CacheFile> outs;
    private boolean async;
    private IsolateConcatOutputStream teeOutput;
    private CloudFileWriter cloudWriter = null;
    private String key, bucket;
    private TierManager parentManager;
    private long version = 0;
    private boolean mustWriteToS3 = true;    //eventually this file has to be written to s3
    private CacheFile bestfile = null;        //best optimal file for asynchronous upload
    private boolean removedPendingFile = false;
    private ArrayList<MultiBlockOutputStream> outOfMemStreams;
    private byte[] oneByteBuffer = new byte[1];
    private CloudFileReader cloudReader = null; // needed in order to retrieve relative information (e.g. last
                                                // modification date of data etc)
    private CacheFile evictedFile = null;
    private boolean isClosed = false;

    private CacheFile s3DummyCacheFile;

    //S3FileWriter cannot be implementation of CacheFile class so it is given separately

    //----------------output only to cache - no s3 -----------------
    CacheOutputStream(CacheFile fileOne, TierManager tmgr) throws IOException {
        outOfMemStreams = new ArrayList<>();
        this.outs = new ArrayList<>(1);
        setNoS3Upload();    //we dont want to re-upload back to s3
        outs.add(fileOne);
        teeOutput = new IsolateConcatOutputStream(fileOne.getOutputStream(), outOfMemStreams);
        parentManager = tmgr;
        setBucketKey();        //retrieve bucket and key from cache files
    }

    //output only to cache - no s3
    CacheOutputStream(CacheFile fileOne, CacheFile fileTwo, TierManager tmgr) throws IOException {
        outOfMemStreams = new ArrayList<>();
        this.outs = new ArrayList<>(2);
        setNoS3Upload();    //we dont want to re-upload back to s3
        outs.add(fileOne);
        outs.add(fileTwo);
        teeOutput = new IsolateConcatOutputStream(fileOne.getOutputStream(), fileTwo.getOutputStream(), outOfMemStreams);
        parentManager = tmgr;
        setBucketKey();        //retrieve bucket and key from cache files
    }

    //---------------- - RANGE - output only to cache - no s3 -----------------
    CacheOutputStream(CacheFile fileOne, long start, long stop, TierManager tmgr) throws IOException {
        outOfMemStreams = new ArrayList<>();
        this.outs = new ArrayList<>(1);
        setNoS3Upload();    //we dont want to re-upload back to s3
        outs.add(fileOne);
        teeOutput = new IsolateConcatOutputStream(fileOne.getOutputStream(start, stop), outOfMemStreams);
        parentManager = tmgr;
        setBucketKey();        //retrieve bucket and key from cache files
    }

    //output only to cache - no s3
    CacheOutputStream(CacheFile fileOne, CacheFile fileTwo, long start, long stop, TierManager tmgr) throws IOException {
        outOfMemStreams = new ArrayList<>();
        this.outs = new ArrayList<>(2);
        setNoS3Upload();    //we dont want to re-upload back to s3
        outs.add(fileOne);
        outs.add(fileTwo);
        teeOutput = new IsolateConcatOutputStream(fileOne.getOutputStream(start, stop),
                fileTwo.getOutputStream(start, stop), outOfMemStreams);
        parentManager = tmgr;
        setBucketKey();        //retrieve bucket and key from cache files
    }

    /* ------------------ Output to Cache & S3 -------------------- */
    //output to cache file (one file) and s3 - async or synchronous
    CacheOutputStream(CacheFile fileOne, CloudFileWriter s3file, boolean async, TierManager tmgr) throws IOException {
        s3DummyCacheFile = CacheFileHelper.createS3CacheFile(s3file.getCloudFile());
        outOfMemStreams = new ArrayList<>();
        this.outs = new ArrayList<>(1);
        this.async = async;
        this.cloudWriter = s3file;
        outs.add(fileOne);

        teeOutput = !async ?
                new IsolateConcatOutputStream(fileOne.getOutputStream(), s3file, outOfMemStreams) :  //final output along with the join of s3 stream
                new IsolateConcatOutputStream(fileOne.getOutputStream(), outOfMemStreams);

        parentManager = tmgr;
        setBucketKey();        //retrieve bucket and key from cache files
    }

    //output to cache file (two files) and s3 - async or synchronous
    CacheOutputStream(CacheFile fileOne, CacheFile fileTwo, CloudFileWriter s3file, boolean async, TierManager tmgr)
            throws IOException {
        s3DummyCacheFile = CacheFileHelper.createS3CacheFile(s3file.getCloudFile());
        outOfMemStreams = new ArrayList<>();
        this.outs = new ArrayList<>(2);
        this.async = async;
        outs.add(fileOne);
        outs.add(fileTwo);
        this.cloudWriter = s3file;

        teeOutput = async ?
                new IsolateConcatOutputStream(fileOne.getOutputStream(), fileTwo.getOutputStream(), outOfMemStreams) :
                new IsolateConcatOutputStream(fileOne.getOutputStream(), fileTwo.getOutputStream(), s3file, outOfMemStreams);            //final output along with the join of third stream

        parentManager = tmgr;
        setBucketKey();        //retrieve bucket and key from cache files
    }

    //output directly to s3 - sync only
    CacheOutputStream(CloudFileWriter s3file, TierManager tmgr) {
        s3DummyCacheFile = CacheFileHelper.createS3CacheFile(s3file.getCloudFile());
        this.outs = new ArrayList<>(1);
        this.async = false;
        this.cloudWriter = s3file;
        teeOutput = new IsolateConcatOutputStream(s3file);
        parentManager = tmgr;
        setBucketKey();        //retrieve bucket and key from cache files
    }

    CacheOutputStream() {
        /* Recovery */
    }

    public boolean isAsync() {
        return async;
    }

    public boolean isGoingManual() {
        if (async)
            return false;
        else
            return cloudWriter.isGoingManual();
    }

    public void abort() {
        if (!isClosed) {
            teeOutput.abort();
            if (!teeOutput.hasS3Stream())    // if teeOutput has not s3file it is our responsibility to close it
                cloudWriter.abort();
            isClosed = true;
        }
    }

    void setCloudFileReader(CloudFileReader s3Reader) {
        this.cloudReader = s3Reader;
    }

    private void setBucketKey() {
        if (outs.size() > 0) {
            bucket = outs.get(0).getBucket();
            key = outs.get(0).getKey();
        } else {
            CloudFile cloudFile = cloudWriter.getCloudFile();
            bucket = cloudFile.getBucket();
            key = cloudFile.getKey();
        }
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    private void setVersion() {
        for (CacheFile file : outs)
            if (file.getSize() > 0) file.setVersion(version);    //if size == 0 it means it will be deleted

        if (cloudWriter != null) // Note: We don't check async to see if s3file exists, because async will not
                                 // matter if it is changed by setAsync (This happen where reading from s3 and
                                 // writing to cache in the same time)
            cloudWriter.getCloudFile().setVersion(version);
    }

    void setEvictedFile(CacheFile ef) {
        evictedFile = ef;
    }

    private void getFilesVersion() {
        for (CacheFile file : outs) {
            version = file.getVersion();
            if (version > 0) break;
        }
    }

    private void setNoS3Upload() {
        this.async = false;    //because we don't want to be enqueued for async uploading
        mustWriteToS3 = false;
    }

    public boolean isUploading() {
        /* S3file is null in case of asynchronous upload */
        if (cloudWriter != null)
            return cloudWriter.isUploadingMultiPart();
        else return false;
    }

    public void uploadLastPart() throws Exception {
        /* S3file is null in case of asynchronous upload */
        if (cloudWriter != null)
            try {
                cloudWriter.uploadLastPart();
        } catch (IOException e) {
            throw new Exception(e);
        }
    }

    public boolean isNotClosed() {
        return !isClosed;
    }

    /**
     * Select the most optimal file for reading (memory if available)
     */
    public CacheFile getOptimalFile() {
        return bestfile;
    }

    private void deleteFileIfIncomplete() {
        if (!removedPendingFile) parentManager.removePending(bucket, key);

        for (CacheFile out : outs) out.deleteIfIncomplete();
    }

    private void sendPreviousData(MultiBlockOutputStream stream) throws IOException {
        CacheFile cf = stream.getCacheFile();
        InputStream in = cf.getIncompleteInputStream();
        if (in != null) {   //if data wrote on file at all (before the occurrence of out of memory exception)
            //block while sending data to s3
            byte[] blockBuffer = new byte[ServerConfigurations.getS3BlockAndUploadBufferSize()];
            while (in.available() > 0) {
                int r = in.read(blockBuffer);
                if (r > 0) teeOutput.write(blockBuffer, 0, r);
            }
            in.close();
        }
    }

    private void outOfMemoryHandler(byte[] buffer, int offset, int len, OutOfMemoryTierType outOfMemType) throws IOException {
        if (mustWriteToS3) {
            if (outOfMemStreams.size() == 2) {   //cache memory full on disk & memory
                //close existing out of memory streams
                for (MultiBlockOutputStream stream : outOfMemStreams)
                    stream.close();

                teeOutput = new IsolateConcatOutputStream(cloudWriter); // replace output with s3file

                if (async) {
                    async = false;
                    sendPreviousData(outOfMemStreams.get(0));
                    teeOutput.write(buffer, offset, len);
                } //else	->  we do nothing. when the stream is synchronous, the previous data + current buffer already sent to s3
            } else if (outOfMemStreams.size() == 1) {
                outOfMemStreams.get(0).close();    //make block visible immediately
                teeOutput.detachStream(outOfMemStreams.get(0));

                if (outs.size() < 2) { //	we stream to only one cache file, and is now full
                    if (outOfMemStreams.get(0).getCacheFile().type() == CacheType.DISK_FILE || !ContinueToDiskPolicy.toDisk()) {
                        teeOutput = new IsolateConcatOutputStream(cloudWriter); // replace output with s3file

                        if (async) {
                            async = false;
                            sendPreviousData(outOfMemStreams.get(0));
                            teeOutput.write(buffer, offset, len);
                        } //else	->  we do nothing. when the stream is synchronous, the previous data + current buffer already sent to s3
                    } else {   //decision is to continue to disk
                        continueToDisk(buffer, offset, len);

                        if (async) {
                            async = false;
                            sendPreviousData(outOfMemStreams.get(0));
                            cloudWriter.write(buffer, offset, len);
                        }
                    }
                }
            }
        } else {
            //	close existing out of memory streams & detach from main out stream
            for (MultiBlockOutputStream stream : outOfMemStreams) {
                if (!stream.isClosed()) {
                    stream.close();
                    teeOutput.detachStream(stream);
                }
            }

            //	downgrade to disk if needed
            if (outs.size() == 1 && outOfMemType == OutOfMemoryTierType.MEMORY && ContinueToDiskPolicy.toDisk())    //we also check outs because we don't want to re-add disk if it was detached before because of out of disk earlier
                continueToDisk(buffer, offset, len);
        }
    }

    private void continueToDisk(byte[] buffer, int offset, int len) throws IOException {
        //if disk is out of memory too, the next write will handle it
        MultiBlockOutputStream newStream;
        long version = outOfMemStreams.get(0).getCacheFile().getVersion();
        DiskManager dmgr = parentManager.getDiskManager();
        CacheFile currentDiskFile = dmgr.getFile(bucket, key);
        if (currentDiskFile != null && currentDiskFile.getVersion() == version) {
            newStream = currentDiskFile.getOutputStream(outOfMemStreams.get(0).getCurrentStart(), outOfMemStreams.get(0).getFinalStop());
        } else {
            currentDiskFile = dmgr.create(bucket, key, 0);
            newStream = currentDiskFile.getOutputStream(outOfMemStreams.get(0).getCurrentStart(), outOfMemStreams.get(0).getFinalStop());
            if (version > 0) currentDiskFile.setVersion(version);
        }

        outs.add(currentDiskFile);
        try {
            newStream.write(buffer, offset, len);
            teeOutput.addStream(newStream);
        } catch (CacheOutOfMemoryException e) {
            outs.remove(currentDiskFile);
        }
    }

    public void write(int b) throws IOException {
        try {
            OutOfMemoryTierType etype = teeOutput.write(b);
            if (etype != OutOfMemoryTierType.NONE) {   //out of memory occurred
                oneByteBuffer[0] = (byte) b;
                outOfMemoryHandler(oneByteBuffer, 0, 1, etype);    //handle in case memory is full
            }
        } catch (IOException e) {
            deleteFileIfIncomplete();
            throw new IOException(e);
        }
    }

    public void write(byte[] buffer) throws IOException {
        try {
            OutOfMemoryTierType etype = teeOutput.write(buffer);
            if (etype != OutOfMemoryTierType.NONE) { //out of memory occurred
                outOfMemoryHandler(buffer, 0, buffer.length, etype); //handle in case memory is full
            }
        } catch (IOException e) {
            deleteFileIfIncomplete();
            throw new IOException(e);
        }
    }

    public void write(byte[] buffer, int offset, int len) throws IOException {
        try {
            OutOfMemoryTierType etype = teeOutput.write(buffer, offset, len);
            if (etype != OutOfMemoryTierType.NONE) {  //out of memory occurred
                outOfMemoryHandler(buffer, offset, len, etype); //handle in case memory is full
            }
        } catch (IOException e) {
            deleteFileIfIncomplete();
            throw new IOException(e);
        }
    }

    public void flush() throws IOException {
        teeOutput.flush();
    }

    public void close() throws IOException {
        boolean s3Succeeded = false;
        teeOutput.flush();
        teeOutput.close();
        Iterator<CacheFile> iter = outs.iterator();
        oneByteBuffer = null;
        isClosed = true;

        /* LOCK */
        CacheOutputStream.lock(bucket, key);

        try {

        /* GET VERSION */
        getFilesVersion();    //get version number from files
        if (version == 0 || cloudWriter != null) {
            version = TierManager.getVersion(bucket, key);    //request a version - Verion exist in a second close of this stream (async upload)
            setVersion();        //set a version number to all files of this stream
        }

        if (logger.isDebugEnabled()) logger.info("---> Lock on:" + key + ": V" + version);

        /* TX Stream (s3, cache) */
        if (!mustWriteToS3) { //only caching, do not upload (used by the read() to store stream into the cache while reading from s3)
            while (iter.hasNext()) {
                CacheFile file = iter.next();
                if (file.getSize() > 0) { //in case file is created but maybe because out of memory didn't got any data at all
                    if (cloudReader != null) {
                        CloudFile cloudFile = cloudReader.getCloudFile();
                        file.setLastModified(cloudFile.getLastModified());
                        file.setActualSize(cloudFile.getLength());
                        if (file.getSize() < cloudFile.getLength())
                            file.setPartialFile();    //	Client Disconnected, file is closed but not it's not the whole file is saved on cache
                    } else if (evictedFile != null)    //because recovery memory file using disk also ends up here, we have to check if there is an eviction file
                    {
                        file.setLastModified(evictedFile.getLastModified());
                        file.setActualSize(evictedFile.getActualSize());
                        evictedFile = null;
                    }
                    /*Warning: Completion must be after the above statements (the block files has to know before completion if there are partial files or not (in order to name the filanem correctly)*/
                    file.stateComplete();
                } else {
                    if (logger.isDebugEnabled()) logger.info("Out of memory - Empty file is going for deletion");
                    file.delete();    //the cache file is empty (because of out of memory) so we must not make visible an empty Cache file
                    //(even it does not reserves any actual bytes on memory or disk. If we make it visible, a read(b,k)
                    //will create a problem because we will have a visible cache file without any actual file behind)
                }

            }

        } else if (!async) {                   //synchronous write to s3 and or cache
            cloudWriter.close();
            s3Succeeded = cloudWriter.completeFile(); // Try to upload to s3 & make visible the s3 file to s3 manager
            if (s3Succeeded) {  //complete files, only if s3 completes
                while (iter.hasNext()) {
                    CacheFile file = iter.next();
                    if (file.getSize() > 0) { //in case file is created but maybe because out of memory didn't got any data at all
                        CloudFile cloudFile = cloudWriter.getCloudFile();
                        file.stateComplete();
                        file.setLastModified(cloudFile.getLastModified());
                        file.setActualSize(cloudFile.getLength());
                    } else file.delete();
                }
            } else { //if s3 fail to complete, delete files
                while (iter.hasNext())
                    iter.next().delete();

                throw new IOException("Close Failed");
            }

            //the removePending should be called after the finalization of upload & put methods of each CacheFile
            if (!removedPendingFile) parentManager.removePending(bucket, key);

        } else { //asynchronous write

            while (iter.hasNext()) {
                CacheFile cfile = iter.next();
                //selecting the optimal file for reading while uploading to s3 later
                //we set "pushed" state only to the enqueued file - for async upload. The other file (not enqueued) will be in complete state, because nobody will change it's state later (it will not be in queue)
                if (outs.size() == 1) {
                    cfile.stateToBePushed();
                    bestfile = cfile;
                }    //enqueue either a disk or memory file
                else if (outs.size() == 2 && cfile.type() == CacheType.MEMORY_FILE) {
                    cfile.stateToBePushed();
                    bestfile = cfile;
                }    //enqueue the memory file (instead of disk file) for performance reasons
                else
                    cfile.stateComplete();    //if we push the memoryfile, complete the disk file. The memory file will be completed by the closing of asynchronous write to s3
            }
        }

        /* we notify all managers about the successful put */
        // The async==true condition is  missing from the below statement because we check and displace the cache files when the file is uploaded on s3, which eventually will be done later

        if ((!async && s3Succeeded) || !mustWriteToS3) parentManager.checkVersion(bucket, key, version);

        if (async) {
            async = false;    //prevent from infinite uploading to s3 - async = false because the next time this stream will be used, will be used as synchronous
            teeOutput = new IsolateConcatOutputStream(cloudWriter); // next time the stream will be used, we will write
                                                                    // to s3 instead of cache
            AsyncCloudUploadManager.enqueue(this); // enqueue for uploading to s3 asynchronously. Bestfile means, first
                                                   // comes the memory file or disk file if there is no memory file
        }

        if (logger.isDebugEnabled()) logger.info("---> UNLock on:" + key + ": V" + version);

    } finally {
        /* UNLOCK */
        CacheOutputStream.unlock(bucket, key);
    }
}

void initiateRecovery(CloudFileWriter s3file, CacheFile fileOne, TierManager tier) {
        outOfMemStreams = new ArrayList<>();
        this.outs = new ArrayList<>(1);
        this.async = false;
        this.cloudWriter = s3file;
        outs.add(fileOne);
        parentManager = tier;
        setBucketKey();        //retrieve bucket and key from cache files

        bestfile = outs.get(0);    //disk is the only best file in case of recovery
        version = bestfile.getVersion();
        s3file.getCloudFile().setVersion(version);
        teeOutput = new IsolateConcatOutputStream(s3file);        //begin to write to s3
        AsyncCloudUploadManager.enqueue(this);
    }

    public boolean isUploadingWithLength() {
        return cloudWriter.isUploadingWithLength();
    }

    public void waitUploadWithLength(int timeout) throws InterruptedException {
        cloudWriter.waitUploadWithLength(timeout);
    }

    public List<CacheFile> getCacheFiles() {
        return outs;
    }

    public CacheFile getS3CacheFile() {
        if (s3DummyCacheFile == null) {
            if (outs.size() == 2) {
                s3DummyCacheFile = CacheFileHelper.createS3CacheFile(outs.get(1));
            } else if (outs.size() == 1) {
                s3DummyCacheFile = CacheFileHelper.createS3CacheFile(outs.get(0));
            }
        }
        return s3DummyCacheFile;
    }

}
