package edu.cut.smacc.server.cache.common;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for a CacheBlock
 *
 * @author Theodoros Danos
 */
public interface CacheBlock {
    long available();

    void write(byte[] buffer, int offset, int len) throws IOException;

    void write(int c) throws IOException;

    boolean close() throws IOException;

    InputStream getFileInputStream() throws IOException;

    void complete();

    void toBePushed();

    void setVersion(long version);

    boolean isComplete();

    boolean isIncomplete();

    boolean isPushed();

    boolean isObsolete();

    void delete();

    long getSize();

    BlockRange getRange();

    void setStateFileObsolete();

    void abortWrite();
}