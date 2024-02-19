package edu.cut.smacc.server.protocol;

import edu.cut.smacc.server.cache.common.BlockRange;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author Michail Boronikolas
 */
public class HeaderServer {

    private final RequestType requestType;
    private String bucket;
    private String key;
    private boolean uploadAsync;
    private Long connectionId;
    private boolean goingManual;
    private String listPrefix;
    private Long length;
    private BlockRange range;

    public HeaderServer(DataInputStream in) throws IOException {
        requestType = RequestType.getRequestType(in.read());
        switch (requestType) {
            case PUT -> {
                bucket = new String(in.readNBytes(in.read()));
                key = new String(in.readNBytes(in.read()));
                uploadAsync = in.readBoolean();
                connectionId = in.readBoolean() ? in.readLong() : null;
                goingManual = in.readBoolean();
                length = in.readBoolean() ? in.readLong() : null;
            }
            case GET -> {
                bucket = new String(in.readNBytes(in.read()));
                key = new String(in.readNBytes(in.read()));
                if (in.readBoolean()) {
                    range = new BlockRange(in.readLong(), in.readLong());
                } else range = null;
            }
            case DEL, DEL_CACHE, FILE_STATUS -> {
                bucket = new String(in.readNBytes(in.read()));
                key = new String(in.readNBytes(in.read()));
            }
            case LIST_CACHE -> {
                bucket = new String(in.readNBytes(in.read()));
                listPrefix = new String(in.readNBytes(in.read()));
            }
            case COLLECT_STATS, RESET_STATS, CLEAR_CACHE, SHUTDOWN -> {
                // Nothing to do
            }

            default -> throw new IOException("Bad enum type...");
        }
    }

    public String getKey() {
        return key;
    }

    public Long getLength() {
        return length;
    }

    public boolean getUploadAsync() {
        return uploadAsync;
    }

    public String getBucket() {
        return bucket;
    }

    public BlockRange getRange() {
        return range;
    }

    public String getListPrefix() {
        return listPrefix;
    }

    public boolean hasBucket() {
        return bucket != null;
    }

    public Long getConnectionId() {
        return connectionId;
    }

    public boolean hasGoingManual() {
        return goingManual;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public boolean hasRequestType() {
        return requestType != null;
    }

    public boolean hasConnectionId() {
        return connectionId != null;
    }

    public boolean hasListPrefix() {
        return listPrefix.length() != 0;
    }
}
