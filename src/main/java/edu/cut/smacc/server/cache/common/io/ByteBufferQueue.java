package edu.cut.smacc.server.cache.common.io;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Represents a block of memory (essentially a byte array) implemented as a
 * queue of ByteBuffer objects. It is implemented in a way that supports the
 * single-writer/multiple-readers access model.
 * <p>
 * The basic unit of data is a ByteBuffer. Hence, the data can only be appended
 * on ByteBuffer at a time.
 */
public class ByteBufferQueue implements Serializable {

    // Data members
    private final ConcurrentLinkedQueue<ByteBuffer> buffers;
    private long size; // sum of bytes of ByteBuffers
    private long capacity; // sum of capacities of ByteBuffers
    private UsageStats memoryStats;

    public ByteBufferQueue(UsageStats memoryStats) {
        this.buffers = new ConcurrentLinkedQueue<>();
        this.size = 0L;
        this.capacity = 0L;
        this.memoryStats = memoryStats;
    }

    /**
     * Append a ByteBuffer in the queue. This method also flips the ByteBuffer
     * for reading
     *
     * @param bb
     */
    void appendByteByffer(ByteBuffer bb) {
        synchronized (buffers) {
            bb.flip();
            buffers.offer(bb);
            size += bb.limit();
            capacity += bb.capacity();

            // refresh the memoryStats
            memoryStats.increment(bb.limit(), bb.capacity());
        }
    }

    /**
     * @return a read-only iterator for iterating the queue
     */
    public Iterator<ByteBuffer> iterator() {
        synchronized (buffers) {
            return Collections.unmodifiableCollection(buffers).iterator();
        }
    }

    /**
     * @return capacity of buffers in bytes
     */
    public long capacity() {
        synchronized (buffers) {
            return capacity;
        }
    }

    /**
     * @return size of data in bytes
     */
    public long size() {
        synchronized (buffers) {
            return size;
        }
    }

    /**
     * Delete all data and return the ByteBuffers back to the pool
     *
     * @param pool
     */
    public void delete(ByteBufferPool pool) {

        synchronized (buffers) {
            // Release all ByteBuffers back to the pool
            while (!buffers.isEmpty()) {
                ByteBuffer byteBuffer = buffers.poll();
                memoryStats.decrement(byteBuffer.limit(), byteBuffer.capacity());

                pool.releaseByteBuffer(byteBuffer);
            }

            capacity = 0;
            size = 0;
        }
    }
}
