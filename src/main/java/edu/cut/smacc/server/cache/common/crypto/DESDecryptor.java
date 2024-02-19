package edu.cut.smacc.server.cache.common.crypto;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Helps to decrypt data that are encrypted using DES cryptographic encryption cipher
 *
 * @author Theodoros Danos
 */
public class DESDecryptor extends InputStream {

    private Cipher des;
    private InputStream in;
    private byte[] internalBuffer;
    private byte[] leftBytes = null;
    private int leftBytesOffset = 0;

    /**
     * @param in       - The encrypted input stream
     * @param password - The password to decrypt the encrypted data
     */
    public DESDecryptor(InputStream in, String password) throws Exception {
        if (password.length() < 8) throw new IOException("DES Password must be at least 8 characters long");

        byte[] desKeyData = password.getBytes();
        this.in = in;
        internalBuffer = new byte[64];

        //	Creating Key
        DESKeySpec desKeySpec = new DESKeySpec(desKeyData);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
        SecretKey desKey = keyFactory.generateSecret(desKeySpec);

        //	Read the IV
        DataInputStream din = new DataInputStream(in);
        int initVectorSize = din.readInt();
        byte[] initVector = new byte[initVectorSize];
        din.readFully(initVector);
        IvParameterSpec ivParameters = new IvParameterSpec(initVector);

        des = Cipher.getInstance("DES/CBC/PKCS5Padding");
        des.init(Cipher.DECRYPT_MODE, desKey, ivParameters);
    }

    public int read() throws IOException {
        int bread = in.read(internalBuffer);
        if (bread > 0) {
            byte[] output = des.update(internalBuffer, 0, 1);
            if (output != null) return output[0];
        } else {
            byte[] output = null;
            try {
                output = des.doFinal();
            } catch (Exception ignored) {
            }

            if (output != null) leftBytes = output;

            if (leftBytes != null && leftBytesOffset <= leftBytes.length - 1) {
                return leftBytes[leftBytesOffset++];
            }
        }
        return -1;
    }

    public int read(byte[] buff) throws IOException {
        int maxRead = internalBuffer.length > buff.length ? buff.length : internalBuffer.length;
        int bread = in.read(internalBuffer, 0, maxRead);
        if (bread > 0) {
            byte[] output = des.update(internalBuffer, 0, bread);
            if (output != null) {
                System.arraycopy(output, 0, buff, 0, output.length);
                return output.length;
            }
        } else {
            byte[] output = null;
            try {
                output = des.doFinal();
            } catch (Exception ignored) {
            }
            if (output != null) {
                System.arraycopy(output, 0, buff, 0, output.length);
                return output.length;
            }
        }
        return 0;
    }

    public int read(byte[] buff, int offset, int len) throws IOException {
        int maxRead = internalBuffer.length > len ? len : internalBuffer.length;
        int bread = in.read(internalBuffer, 0, maxRead);
        if (bread > 0) {
            byte[] output = des.update(internalBuffer, 0, bread);
            if (output != null) {
                System.arraycopy(output, 0, buff, offset, output.length);
                return output.length;
            }
        } else {
            byte[] output = null;
            try {
                output = des.doFinal();
            } catch (Exception ignored) {
            }
            if (output != null) {
                System.arraycopy(output, 0, buff, offset, output.length);
                return output.length;
            }
        }
        return 0;
    }

    public void close() throws IOException {
        in.close();
    }
}
