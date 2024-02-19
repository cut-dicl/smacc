package edu.cut.smacc.test.communication;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

public class MyInputStream extends InputStream {

    private final int defaultArraySize = 1024;
    private final LinkedList<byte[]> list = new LinkedList<>();
    private final long length;

    private int readArrayPosition = 0;
    private int writeArrayPosition = 0;

    private long bytesWritten = 0;
    private long bytesRead = 0;

    public MyInputStream(long length) {
        this.length = length;
        list.add(new byte[defaultArraySize]);
    }

    public void write(byte[] bytes, int off, int len) {
        for (int i = off; i < len; i++) {
            if (writeArrayPosition != defaultArraySize) {
                list.getLast()[writeArrayPosition++] = bytes[i];
            } else {
                list.add(new byte[defaultArraySize]);
                list.getLast()[0] = bytes[i];
                writeArrayPosition = 1;
            }
        }

        synchronized (list) {
            list.notify();
        }

        bytesWritten += len - off;
    }

    @Override
    public int read() throws IOException {
        if (bytesRead == length) {
            super.close();
            return -1;
        }

        if (bytesRead == bytesWritten)
            try {
                synchronized (list) {
                    list.wait();
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }

        byte b = list.getFirst()[readArrayPosition];
        readArrayPosition++;

        if (readArrayPosition == defaultArraySize) {
            list.removeFirst();
            readArrayPosition = 0;
        }

        ++bytesRead;

        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int count = 0;

        if (len > b.length)
            throw new IOException("Bad len argument");
        if (off + len > b.length)
            throw new IOException("Bad off or len arguments");

        for (int i = 0; i < len; i++) {

            int x = read();
            if (x == -1) {
                if (i == 0)
                    return -1;
                else
                    break;
            }

            b[off + i] = (byte) x;

            ++count;
        }

        return count;
    }
}
