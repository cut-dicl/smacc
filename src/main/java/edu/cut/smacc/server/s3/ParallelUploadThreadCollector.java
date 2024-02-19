package edu.cut.smacc.server.s3;

import com.amazonaws.services.s3.model.PartETag;

import java.util.*;
import java.util.concurrent.Future;

/**
 * Collects finished threads or cancel/abort multiple threads that correspond to a particular upload file
 *
 * @author Theodoros Danos
 */
public class ParallelUploadThreadCollector implements Runnable {

    /* Instance */
    private Map<Future<PartETag>, ParallelUploadPart> globalFutureMap;
    private boolean shutdown = false;
    private S3ParallelUploadHandler handler;
    private final List<String> cancelList;

    ParallelUploadThreadCollector(Map<Future<PartETag>, ParallelUploadPart> globalFutureMap, S3ParallelUploadHandler handler) {
        this.globalFutureMap = globalFutureMap;
        this.handler = handler;
        this.cancelList = Collections.synchronizedList(new ArrayList<>());
    }

    void cancelUploadId(String uploadId) {
        synchronized (cancelList) {
            cancelList.add(uploadId);
        }
    }

    public void shutdown() {
        shutdown = true;
    }

    public void run() {
        while (!shutdown) {
            Set<Future<PartETag>> fset = globalFutureMap.keySet();
            Iterator<Future<PartETag>> iter = fset.iterator();

            String cuid = getCanceledUID();
            Future<PartETag> future;

            while (iter.hasNext()) {
                while (true) {
                    try {
                        future = iter.next();
                        break;
                    } catch (Exception e) {
                        iter = fset.iterator();
                    }
                }
                ParallelUploadPart thread = globalFutureMap.get(future);
                String tUploadId = thread.getUploadId();
                if (tUploadId.equals(cuid)) future.cancel(true);

                if (future.isDone() || future.isCancelled()) {
                    iter.remove();
                    thread.releaseMemory();

                    handler.unlockParallelUploadCounter();
                    handler.addCollectedFuture(future, tUploadId);
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) { /* Do nothing */ }
        }
    }

    private String getCanceledUID() {
        synchronized (cancelList) {
            if (!cancelList.isEmpty()) {
                String uid = cancelList.get(0);
                cancelList.remove(0);
                return uid;
            } else return null;
        }
    }
}