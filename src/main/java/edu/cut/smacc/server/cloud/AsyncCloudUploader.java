package edu.cut.smacc.server.cloud;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.tier.CacheOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;

/**
 * Uploads the asynchronous cache files to s3
 *
 * @author Theodoros Danos
 */
class AsyncCloudUploader implements Runnable {
    private static final Logger logger = LogManager.getLogger(AsyncCloudUploader.class);

    private CacheOutputStream cstream;

    AsyncCloudUploader(CacheOutputStream cstream) {
        this.cstream = cstream;
    }

    public void run() {
        try {
            String key;
            CacheFile readFile = cstream.getOptimalFile();
            InputStream in = readFile.getInputStream();

            if (in != null) { // file is obsolete - new is available or file is deleted
                key = readFile.getKey();

                if (logger.isDebugEnabled())
                    logger.info("S3 ASYNCHRONOUS WRITING STARTED[v" + readFile.getVersion() + "]:" + ":" + key);

                int bufferSize = ServerConfigurations.getAsynchronousUploadBuffer();
                if (readFile.getSize() < bufferSize)
                    bufferSize = (int) readFile.getSize();
                byte[] buffer = new byte[bufferSize];
                int r;

                while (in.available() > 0) {
                    r = in.read(buffer, 0, bufferSize);
                    cstream.write(buffer, 0, r);
                }

                /* We do not have any more data so send the last buffered data to s3 */
                cstream.uploadLastPart();

                /* Wait for upload of the last data - Non-blocking */
                while (cstream.isUploading()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                }

                while (cstream.isUploadingWithLength()) {
                    try {
                        cstream.waitUploadWithLength(100);
                    } catch (InterruptedException e) {
                    }
                }

                /* Closes s3 upload and complete the pushed cache file */
                cstream.flush();
                cstream.close();
                in.close();

                if (logger.isDebugEnabled())
                    logger.debug("S3 ASYNCHRONOUS WRITTING FINISHED:" + ":" + key + " [v" + readFile.getVersion() + "]:"
                            + ":" + key);
            }
        } catch (Exception e) {
            logger.error(e.getMessage()); /* LOG ALL EXCEPTIONS {ERROR LOG} AND CLOSE */
        }
    }
}