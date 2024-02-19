package edu.cut.smacc.server.main.request;

import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.protocol.HeaderServer;

import java.io.IOException;

/**
 * An interface for SMACC server request handlers.
 */
public interface RequestHandler {

    void handleRequest(HeaderServer header, CloudInfo cloudInfo) throws IOException;
}
