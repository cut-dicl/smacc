package edu.cut.smacc.server.cache.common.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import edu.cut.smacc.configuration.ServerConfigurations;

public class WritableInputStream extends InputStream {

    private static final int defaultArraySize = ServerConfigurations.getServerToS3BufferSize();
    private static final ByteBufferPool byteBufferPool = new ByteBufferPool(defaultArraySize);

    private final LinkedList<ByteBuffer> list;
    private final long length;

    private int readArrayPosition = 0;

    private long bytesWritten = 0;
    private long bytesRead = 0;

    public WritableInputStream(long length) {
        this.length = length;
        list = new LinkedList<>();
        list.add(byteBufferPool.acquireByteBuffer());
    }

    public void write(byte[] bytes, int off, int len) {
        int bytesToBeWritten = len;
        ByteBuffer lastBB = list.getLast();

        int availableBytesInBB = lastBB.limit() - lastBB.position();
        while (len > availableBytesInBB) {

            lastBB.put(bytes, off, availableBytesInBB);
            off += availableBytesInBB;
            len -= availableBytesInBB;

            list.add(byteBufferPool.acquireByteBuffer());
            lastBB = list.getLast();

            availableBytesInBB = lastBB.limit() - lastBB.position();
        }

        lastBB.put(bytes, off, len);

        bytesWritten += bytesToBeWritten;

        synchronized (list) {
            list.notify();
        }
    }

    @Override
    public int read() throws IOException {
        if (bytesRead == length) {
            super.close();
            return -1;
        }

        if (bytesRead == bytesWritten) try {
            synchronized (list) {
                list.wait();
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        byte b = list.getFirst().get(readArrayPosition++);

        if (readArrayPosition == defaultArraySize) {
            byteBufferPool.releaseByteBuffer(list.removeFirst());

            readArrayPosition = 0;
        }

        ++bytesRead;

        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (off + len > b.length) throw new IOException("Index out of bounds");

        if (bytesRead == length) {
            super.close();
            return -1;
        }

        int bytesToRead = Integer.min(len, (int) (length - bytesRead));

        while (bytesToRead > bytesWritten - bytesRead) try {
            synchronized (list) {
                list.wait();
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        for (int i = 0; i < bytesToRead; i++) {
            if (readArrayPosition == defaultArraySize) {
                byteBufferPool.releaseByteBuffer(list.removeFirst());

                readArrayPosition = 0;
            }

            b[off + i] = list.getFirst().get(readArrayPosition++);
        }

        bytesRead += bytesToRead;

        return bytesToRead;
    }

    @Override
    public void close() {
        while (!list.isEmpty()) {
            byteBufferPool.releaseByteBuffer(list.poll());
        }
    }
}
