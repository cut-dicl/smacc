package edu.cut.smacc.server.cache.common;

import java.io.IOException;

public enum StateType {
    INCOMPLETE(0), TOBEPUSHED(1), COMPLETE(2), OBSOLETE(3);

    private final int stateType;

    public int getInt() {
        return stateType;
    }

    StateType(int stateType) {
        this.stateType = stateType;
    }

    public static StateType getStateType(int stateType) throws IOException {
        switch (stateType) {
            case  0: return INCOMPLETE;
            case  1: return TOBEPUSHED;
            case  2: return COMPLETE;
            case  3: return OBSOLETE;
            default: throw new IOException("Bad enum number...");
        }
    }
}
