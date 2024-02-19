package edu.cut.smacc.server.protocol;

import java.io.IOException;

public enum RequestType {
    PUT(0),
    GET(1),
    DEL(2),
    LIST(3),
    COLLECT_STATS(4),
    SHUTDOWN(5),
    CLEAR_CACHE(6),
    DEL_CACHE(7),
    LIST_CACHE(8),
    FILE_STATUS(9),
    RESET_STATS(10);

    private final int requestType;

    public int getInt() {
        return requestType;
    }

    RequestType(int requestType) {
        this.requestType = requestType;
    }

    public static RequestType getRequestType(int requestType) throws IOException {
        return switch (requestType) {
            case 0 -> PUT;
            case 1 -> GET;
            case 2 -> DEL;
            case 3 -> LIST;
            case 4 -> COLLECT_STATS;
            case 5 -> SHUTDOWN;
            case 6 -> CLEAR_CACHE;
            case 7 -> DEL_CACHE;
            case 8 -> LIST_CACHE;
            case 9 -> FILE_STATUS;
            case 10 -> RESET_STATS;
            default -> throw new IOException("Bad enum number...");
        };
    }
}
