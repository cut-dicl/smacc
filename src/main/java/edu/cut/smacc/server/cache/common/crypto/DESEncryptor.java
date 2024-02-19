package edu.cut.smacc.server.cache.common.crypto;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * helps to encrypt data using DES Encryption algorithm
 *
 * @author everest
 */
public class DESEncryptor extends OutputStream {
    private static final Logger logger = LogManager.getLogger(DESEncryptor.class);

    private DataOutputStream dout;
    private byte[] oba = new byte[1];    // One byte array
    private Cipher des;

    public DESEncryptor(OutputStream out, String password) throws Exception {
        if (password.length() < 8) throw new IOException("DES Password must be at least 8 characters long");

        byte[] desKeyData = password.getBytes();

        //	Creating Key
        DESKeySpec desKeySpec = new DESKeySpec(desKeyData);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
        SecretKey desKey = keyFactory.generateSecret(desKeySpec);

        //	Using DES
        des = Cipher.getInstance("DES/CBC/PKCS5Padding");
        des.init(Cipher.ENCRYPT_MODE, desKey);

        //	Write IV to output
        byte[] initVector = des.getIV();
        dout = new DataOutputStream(out);
        dout.writeInt(initVector.length);
        dout.write(initVector);
    }

    public void write(int b) throws IOException {
        oba[0] = (byte) b;
        byte[] output = des.update(oba, 0, 1);
        if (output != null) dout.write(output);
    }

    public void write(byte[] buff) throws IOException {
        byte[] output = des.update(buff, 0, buff.length);
        if (output != null) dout.write(output);
    }

    public void write(byte[] buff, int offset, int len) throws IOException {
        byte[] output = des.update(buff, offset, len);
        if (output != null) dout.write(output);
    }

    public void close() throws IOException {
        byte[] output = null;
        try {
            output = des.doFinal();
        } catch (Exception e) {
            logger.error("DES Closing Error");
        }
        if (output != null)
            dout.write(output);

        //	Closing Streams
        dout.flush();
        dout.close();
    }
}
