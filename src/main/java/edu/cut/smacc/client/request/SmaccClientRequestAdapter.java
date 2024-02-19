package edu.cut.smacc.client.request;

import java.io.OutputStream;

public class SmaccClientRequestAdapter {

    public static SmaccClientInputStream adaptToInputStream(SmaccClientRequest request) {
        if (request instanceof SmaccClientInputStream) {
            return (SmaccClientInputStream) request;
        } else {
            throw new IllegalArgumentException("The given request cannot be adapted to ClientInputStream");
        }
    }

    public static SmaccClientOutputStream adaptToOutputStream(SmaccClientRequest request) {
        if (request instanceof SmaccClientOutputStream) {
            return (SmaccClientOutputStream) request;
        } else {
            throw new IllegalArgumentException("The given request cannot be adapted to ClientOutputStream");
        }
    }

    public static String getETagFromOutputStream(OutputStream putRequest) {
        return ((SmaccClientOutputStream) putRequest).getETag();
    }
}
