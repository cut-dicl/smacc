package edu.cut.smacc.server.cloud;

import edu.cut.smacc.server.tier.CacheOutputStream;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * helps in asynchronous upload of cache files
 *
 * @author Theodoros Danos
 */
public class AsyncCloudFeeder implements Runnable {
    private static final Logger logger = LogManager.getLogger(AsyncCloudFeeder.class);

    /* ************* INSTANCE *************** */
    private boolean shutdown = false, shutdownAlready = false;
    private ExecutorService service;
    private ArrayList<String> bucketKeys;

    AsyncCloudFeeder(int threads) {
        this.bucketKeys = new ArrayList<>();
        this.service = Executors.newFixedThreadPool(threads);
    }

    protected void shutdown() {
        if (!shutdownAlready) {
            logger.info("Shutting Down ASYNC");
            shutdown = true;
            shutdownAlready = true;
            service.shutdownNow();
            logger.info("ASYNC UPLOAD SERVICE TERMINATED");
        }
    }

    public void run() {
        logger.info("Asynchronous Feeder Started");
        CacheOutputStream cstream;

        while (!shutdown) {
            if (!AsyncCloudUploadManager.isQueueEmpty()) { // dequeue() is non-blocking
                cstream = AsyncCloudUploadManager.dequeue();
                if (cstream == null)
                    continue;
                String bucketkey = cstream.getBucket() + cstream.getKey();
                bucketKeys.add(bucketkey);
                service.execute(new AsyncCloudUploader(cstream));
            } else {
                try {
                    Thread.sleep(100); // sleep until something fill this queue
                } catch (InterruptedException ignored) {
                }
            }
        }
        service.shutdown(); // shutdown submission - when threads close, shutdown executor
    }
}