package edu.cut.smacc.server.cache.common.io;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A pool of reusable ByteBuffer objects
 */
public class ByteBufferPool implements Serializable {

    // Data members
    private ConcurrentLinkedQueue<ByteBuffer> bbPool;
    private int bbCapacity;

    /**
     * @param capacity capacity for each ByteBuffer in bytes
     */
    public ByteBufferPool(int capacity) {
        this.bbCapacity = capacity;
        this.bbPool = new ConcurrentLinkedQueue<>();
    }

    /**
     * @return Either a new ByteBuffer or an existing ByteBuffer from the pool
     */
    ByteBuffer acquireByteBuffer() {

        ByteBuffer buf = bbPool.poll();

        if (buf == null) buf = ByteBuffer.allocate(bbCapacity);  // Create new byte buffer

        return buf;
    }

    /**
     * Return the ByteBuffer into the pool
     *
     * @param bb
     */
    void releaseByteBuffer(ByteBuffer bb) {
        if (bb == null) return;

        bb.clear();
        bbPool.offer(bb);
    }

    /**
     * Clear the pool
     */
    public void clear() {
        bbPool.clear();
    }
}
