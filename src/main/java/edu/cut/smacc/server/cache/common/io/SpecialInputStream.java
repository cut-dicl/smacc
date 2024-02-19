package edu.cut.smacc.server.cache.common.io;

import edu.cut.smacc.server.cache.common.CacheFile;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class helps to decrease a lock that is used in order to know how many readers a file has.
 * When the file is closed or client is crashed this class handles the lock and makes sure to unlock
 * the lock (in order the file to be able to be deleted if there is a need)
 *
 * @author Theodoros Danos
 */
public class SpecialInputStream extends InputStream {
    private InputStream internalin;
    private CacheFile cfile;
    private boolean alreaadyDecreased = false;

    public SpecialInputStream(InputStream in, CacheFile file) {
        super();
        this.internalin = in;
        this.cfile = file;
    }

    public int read() throws IOException {
        try {
            int r = internalin.read();
            if (r < 0) decreaseRead(null);
            return r;
            //make sure that in an error we unlock the file so it can be deleted
        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public int read(byte[] buffer) throws IOException {
        int r;
        try {
            r = internalin.read(buffer);
            if (r <= 0) decreaseRead(null);
            return r;
            //make sure that in an error we unlock the file so it can be deleted
        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public int read(byte[] buffer, int offset, int len) throws IOException {
        int r;
        try {
            r = internalin.read(buffer, offset, len);
            if (r <= 0) decreaseRead(null);
            return r;
            //make sure that in an error we unlock the file so it can be deleted
        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public void reset() throws IOException {
        try {
            internalin.reset();
        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public long skip(long n) throws IOException {
        try {
            return internalin.skip(n);
        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public void mark(int readlimit) {
        internalin.mark(readlimit);
    }

    public boolean markSupported() {
        return internalin.markSupported();
    }

    public int available() throws IOException {
        try {
            if (internalin.available() == 0) decreaseRead(null);
            return internalin.available();

            //make sure that in an error we unlock the file so it can be deleted
        } catch (IOException e) {
            close();
            throw new IOException(e);
        }
    }

    public void close() throws IOException {
        decreaseRead(internalin);
        //the decReadQueue handles to close connection, because it's syn and that makes sure file won't move
    }

    /**
     * Decrease the reader's lock
     *
     * @param in - Inputstream that is used
     * @throws IOException
     */
    private void decreaseRead(InputStream in) throws IOException {
        if (!alreaadyDecreased) {
            alreaadyDecreased = true;
            cfile.decReadQueue(in);
        }
    }

    public CacheFile getCacheFile() {
        return cfile;
    }

}