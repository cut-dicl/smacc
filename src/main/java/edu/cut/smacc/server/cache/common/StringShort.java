package edu.cut.smacc.server.cache.common;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

/**
 * Provides useful methods for manipulating strings
 *
 * @author Theodoros Danos
 */
public class StringShort {
    private static final Logger logger = LogManager.getLogger(StringShort.class);

    /**
     * gets a string and returns a hex reprasantation of that string
     *
     * @param str
     * @return
     */
    static public String toHex(String str) {
        return Hex.encodeHexString(str.getBytes());
    }

    /**
     * receive a byte array and give a hex string representation of that string
     *
     * @param byteArr
     * @return
     */
    static public String toHex(byte[] byteArr) {
        return Hex.encodeHexString(byteArr);
    }

    /**
     * Decodes hexadecimal string into a string (e.g. "ABFF" to "AB")
     *
     * @param encodedStr
     * @return
     */
    static String fromHex(String encodedStr) {
        try {
            byte[] bytes = Hex.decodeHex(encodedStr.toCharArray());
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (DecoderException e) {
            logger.error(e);
            return null;
        }//LOG IT
    }
}