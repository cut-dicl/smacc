package edu.cut.smacc.server.cache.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Describes the range of a particular block
 *
 * @author Theodoros Danos
 */
public class BlockRange {

    private long start, stop, length;

    public BlockRange(long start, long stop) {
        this.start = start;
        this.stop = stop;
        length = stop - start + 1;
    }

    public void update(long start, long stop) {
        this.start = start;
        this.stop = stop;
        length = stop - start + 1;
    }

    public long getStop() {
        return stop;
    }

    public long getStart() {
        return start;
    }

    public long getLength() {
        return length;
    }

    public boolean contains(long rangeIndex) {
        return rangeIndex >= start && rangeIndex <= stop;
    }

    public String toString() {
        return "Start:" + start + " - " + "Stop: " + stop + " - " + "Length: " + length;
    }

    public boolean equals(BlockRange compRange) {
        return compRange.getStart() == start && compRange.getStop() == stop;
    }

    public void send(DataOutputStream out) throws IOException {
        out.writeLong(start);
        out.writeLong(stop);
        out.flush();
    }

    static BlockRange receive(DataInputStream in) throws IOException {
        return new BlockRange(in.readLong(), in.readLong());
    }
}

