package edu.cut.smacc.server.tier;

import edu.cut.smacc.configuration.ServerConfigurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;

/**
 * It is used in recovery in order to transfer a block of one cache file to another block to another cache file
 *
 * @author Theodoros Danos
 */
public class BlockTransferProcessor implements Runnable {
    private static final Logger logger = LogManager.getLogger(BlockTransferProcessor.class);

    private InputStream diskInputStream;
    private CacheOutputStream memoryOutput;
    private TierManager tier;

    BlockTransferProcessor(TierManager tier, InputStream diskInputStream, CacheOutputStream memoryOutput) {
        this.diskInputStream = diskInputStream;
        this.memoryOutput = memoryOutput;
        this.tier = tier;
    }

    public void run() {
        if (logger.isDebugEnabled()) logger.info("Block recovery started");

        /* Wait for recovery procedure to finish */
        while (!tier.isRecoveryDone())
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }

        byte[] buffer = new byte[ServerConfigurations.recoveryBufferSize()];
        try {
            int readBytes = diskInputStream.read(buffer);
            while (readBytes > 0) {
                memoryOutput.write(buffer, 0, readBytes);
                readBytes = diskInputStream.read(buffer);
            }

            memoryOutput.close();
        } catch (Exception e) {
            try {
                diskInputStream.close(); /*make sure to release the read lock*/
            } catch (Exception ex) { /* Do nothing */ }
            logger.error(e.getMessage());
        }

        if (logger.isDebugEnabled()) logger.info("Block recovery finished");
    }
}
