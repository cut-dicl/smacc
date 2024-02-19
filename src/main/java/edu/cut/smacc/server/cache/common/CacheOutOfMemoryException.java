package edu.cut.smacc.server.cache.common;

import java.io.IOException;

public class CacheOutOfMemoryException extends IOException {

    private String message;

    public CacheOutOfMemoryException(String message) {
        super(message);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
