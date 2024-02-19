package edu.cut.smacc.server.s3;

import com.amazonaws.services.s3.model.PartETag;
import edu.cut.smacc.server.cache.common.io.ByteBufferPool;
import edu.cut.smacc.server.cache.common.io.ByteBufferQueue;
import edu.cut.smacc.configuration.ServerConfigurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used when a file is uploading in s3. This class handles the parallel threads for upload of smacc objects
 *
 * @author Theodoros Danos
 */
class S3ParallelUploadHandler {
    private static final Logger logger = LogManager.getLogger(S3ParallelUploadHandler.class);
    private ByteBufferPool pool;
    private ExecutorService autoUploadExecutorService = null;
    private final Map<String, List<Future<PartETag>>> futureLists;    //<UploadId, <List of Futures>>
    private AtomicInteger globalAutoUploadCounter;
    private Map<Future<PartETag>, ParallelUploadPart> globalFutureMap;
    private ParallelUploadThreadCollector tasksCollectionHandler;
    private Thread tasksCollectionHandlerThread;

    S3ParallelUploadHandler(ByteBufferPool pool) {
        futureLists = Collections.synchronizedMap(new HashMap<>());
        globalFutureMap = Collections.synchronizedMap(new HashMap<>());
        this.globalAutoUploadCounter = new AtomicInteger(0);
        this.pool = pool;
    }

    void startHandler(int threadNum) {
        autoUploadExecutorService = Executors.newFixedThreadPool(threadNum);
        tasksCollectionHandler = new ParallelUploadThreadCollector(globalFutureMap, this);
        tasksCollectionHandlerThread = new Thread(tasksCollectionHandler);
        tasksCollectionHandlerThread.start();
    }

    void shutdownHandler() {
        autoUploadExecutorService.shutdownNow();
        tasksCollectionHandler.shutdown();
        tasksCollectionHandlerThread.interrupt();
    }

    void deleteAllParallelUploadFutures(String uploadId) {
        if (futureLists.containsKey(uploadId)) {
            futureLists.get(uploadId).clear();
            futureLists.remove(uploadId);
        }
    }

    void addCollectedFuture(Future<PartETag> future, String uploadId) {
        if (!futureLists.containsKey(uploadId))
            futureLists.put(uploadId, Collections.synchronizedList(new ArrayList<>()));
        futureLists.get(uploadId).add(future);
    }

    void cancelUpload(String uploadId) {
        tasksCollectionHandler.cancelUploadId(uploadId);
    }

    void waitParallelUploadThreads(String uploadId, int waitingThreadsCounter) {
        if (waitingThreadsCounter > 0) {
            while (!futureLists.containsKey(uploadId) || futureLists.get(uploadId) == null || futureLists.get(uploadId).size() < waitingThreadsCounter) {
                try {
                    Thread.sleep(400);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    boolean isUploading(String uploadId, int waitingThreadsCounter) {
        if (waitingThreadsCounter > 0) {
            return !futureLists.containsKey(uploadId) || futureLists.get(uploadId) == null || futureLists.get(uploadId).size() < waitingThreadsCounter;
        } else return false;
    }

    List<Future<PartETag>> getParallelUploadFutures(String uploadId) {
        return futureLists.getOrDefault(uploadId, null);
    }

    private boolean tryLockParallelUploadCounter(boolean lastPart) {
        synchronized (futureLists)    //synchronized because of the global counter
        {
            if (!lastPart && globalAutoUploadCounter.intValue() >= ServerConfigurations.getS3MaxParallelUploadGlobal())
                return false;
            else {
                if (logger.isDebugEnabled()) logger.info("#########INCREASE LOCK!");
                globalAutoUploadCounter.incrementAndGet();
            }
        }
        return true;
    }

    void unlockParallelUploadCounter() {
        if (logger.isDebugEnabled()) logger.info("********DECREASE LOCK!");
        globalAutoUploadCounter.decrementAndGet();
    }

    boolean tryParallelUpload(S3FileWriter file, boolean lastPart, ByteBufferQueue queue, int bufferSize, int currentpartId) {
        String uploadId = file.getUploadId();
        if (!tryLockParallelUploadCounter(lastPart)) return false;

        if (logger.isDebugEnabled()) logger.info("GLOBAL AUTO UPL COUNTER: " + globalAutoUploadCounter.intValue());
        ParallelUploadPart autoUploadPart = new ParallelUploadPart(lastPart, queue, uploadId, currentpartId, file.getBucket(), file.getKey(), file.getS3Client(), bufferSize, pool);
        globalFutureMap.put(autoUploadExecutorService.submit(autoUploadPart), autoUploadPart);
        return true;
    }

    void setParallelUpload(S3FileWriter file, boolean lastPart, ByteBufferQueue queue, int bufferSize, int currentpartId) {
        String uploadId = file.getUploadId();

        if (logger.isDebugEnabled()) logger.info("GLOBAL AUTO UPL COUNTER: " + globalAutoUploadCounter.intValue());
        ParallelUploadPart autoUploadPart = new ParallelUploadPart(lastPart, queue, uploadId, currentpartId, file.getBucket(), file.getKey(), file.getS3Client(), bufferSize, pool);
        globalFutureMap.put(autoUploadExecutorService.submit(autoUploadPart), autoUploadPart);
    }

    boolean acquireGlobalLock() {
        synchronized (futureLists) {
            if (globalAutoUploadCounter.intValue() < ServerConfigurations.getS3MaxParallelUploadGlobal()) {
                if (logger.isDebugEnabled()) logger.info("#########INCREASE LOCK!");
                globalAutoUploadCounter.incrementAndGet();
                return true;
            } else
                return false;
        }
    }
}