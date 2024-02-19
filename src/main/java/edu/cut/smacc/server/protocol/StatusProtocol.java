package edu.cut.smacc.server.protocol;

import java.io.DataInputStream;
import java.io.IOException;


public class StatusProtocol {

    private boolean success;
    private boolean keepAlive;
    private boolean goingManual;
    private Long connectionId;
    private String exceptionMessage;

    public boolean getFailure() {
        return !success;
    }

    public boolean hasKeepAlive() {
        return keepAlive;
    }

    public Long getConnectionId() {
        return connectionId;
    }

    public boolean hasGoingManual() {
        return goingManual;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public boolean hasConnectionId() {
        return connectionId != null;
    }

    public boolean hasExceptionMessage() {
        return exceptionMessage.length() != 0;
    }

    public StatusProtocol(DataInputStream in) throws IOException {
        read(in);
    }

    public void read(DataInputStream in) throws IOException {
        success = in.readBoolean();
        keepAlive = in.readBoolean();
        goingManual = in.readBoolean();
        connectionId = in.readBoolean() ? in.readLong() : null;
        exceptionMessage = new String(in.readNBytes(in.readShort()));
    }

}
