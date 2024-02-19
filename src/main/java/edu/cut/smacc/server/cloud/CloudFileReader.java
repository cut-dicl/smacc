package edu.cut.smacc.server.cloud;

import java.io.InputStream;

/**
 * An abstract InputStream for reading a cloud file
 */
public abstract class CloudFileReader extends InputStream {

    public abstract CloudFile getCloudFile();
}
