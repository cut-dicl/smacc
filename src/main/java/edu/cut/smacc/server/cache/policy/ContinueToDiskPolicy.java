package edu.cut.smacc.server.cache.policy;

/**
 * This class provides a way for a policy to decide if a memory file is going to continue to write to disk or not
 *
 * @author Theodoros Danos
 */
public class ContinueToDiskPolicy {
    public static boolean toDisk() {
        return false;
    }
}
