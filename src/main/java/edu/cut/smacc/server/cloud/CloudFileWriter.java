package edu.cut.smacc.server.cloud;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An abstract OutputStream for writing to a cloud file
 */
public abstract class CloudFileWriter extends OutputStream {

    public abstract CloudFile getCloudFile();

    public abstract boolean completeFile();

    public abstract boolean isUploadingWithLength();

    public abstract void waitUploadWithLength(int timeout) throws InterruptedException;

    public abstract boolean isUploadingMultiPart();

    public abstract void abort();

    public abstract boolean isGoingManual();

    public abstract void uploadLastPart() throws IOException;
}
