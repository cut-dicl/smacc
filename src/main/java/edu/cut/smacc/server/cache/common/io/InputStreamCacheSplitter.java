package edu.cut.smacc.server.cache.common.io;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.s3.S3File;
import edu.cut.smacc.server.tier.CacheOutputStream;
import org.apache.commons.io.input.TeeInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class InputStreamCacheSplitter extends InputStream {
    private final OutputStream out;
    private final TeeInputStream finalInputStream;

    //THIS READER MUST NOT CHANGE FOR THE SAKE OF CHUNKS

    public InputStreamCacheSplitter(InputStream s3in, OutputStream out) {
        this.out = out;
        finalInputStream = new TeeInputStream(s3in, out);
    }

    public int read() throws IOException {
        return finalInputStream.read();
    }

    public int read(byte[] buff) throws IOException {
        return finalInputStream.read(buff);
    }

    public int read(byte[] buff, int off, int len) throws IOException {
        return finalInputStream.read(buff, off, len);
    }

    public void close() throws IOException {
        out.close();    //make sure the output stream is closed. Do not let it handled by teeInputStream class
        finalInputStream.close();
    }

    public long skip(long bytes) throws IOException {
        return finalInputStream.skip(bytes);
    }

    public List<CacheFile> getCacheFiles() {
        CacheOutputStream cacheOutputStream = (CacheOutputStream) out;
        return cacheOutputStream.getCacheFiles();
    }

    public S3File getS3CacheFile() {
        CacheOutputStream cacheOutputStream = (CacheOutputStream) out;
        return (S3File) cacheOutputStream.getS3CacheFile();
    }

}
