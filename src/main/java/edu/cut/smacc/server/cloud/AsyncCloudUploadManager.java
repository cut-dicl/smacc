package edu.cut.smacc.server.cloud;

import edu.cut.smacc.server.tier.CacheOutputStream;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * used in order to synchronize the s3 upload queue
 *
 * @author Theodoros Danos
 */
public class AsyncCloudUploadManager {
    /* ****** STATIC ****** */
    private static ConcurrentLinkedQueue<CacheOutputStream> filequeue = new ConcurrentLinkedQueue<>();// synchronized
                                                                                                      // queue

    public static void enqueue(CacheOutputStream cacheOutputStream) {
        filequeue.add(cacheOutputStream);
    }

    static CacheOutputStream dequeue() {
        return filequeue.poll();
    }

    static boolean isQueueEmpty() {
        return filequeue.isEmpty();
    }

    private AsyncCloudFeeder feederRun;

    void StartUploader(int threads) {
        feederRun = new AsyncCloudFeeder(threads);
        Thread feederThread = new Thread(feederRun);
        feederThread.start();
    }

    void shutdownUploader() {
        feederRun.shutdown();
    }
}