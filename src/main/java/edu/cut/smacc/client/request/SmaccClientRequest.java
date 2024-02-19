package edu.cut.smacc.client.request;

import java.io.IOException;

/**
 * A simple interface for all client requests
 */
public interface SmaccClientRequest {

    /**
     * Initiate the request by connecting to the server and sending the request header
     * @throws IOException if the request fails
     */
    boolean initiate() throws IOException;

    /**
     * Close the connection to the server
     */
    void close() throws IOException;
}
