package edu.cut.smacc.utils;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * A set of useful static methods
 *
 * Code taken from: org.apache.hadoop.util.ReflectionUtils
 */
public class ReflectionUtils {

    private static final Class<?>[] EMPTY_ARRAY = new Class[] {};

    // Cache of constructors for each class
    private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new HashMap<Class<?>, Constructor<?>>();

    /**
     * Create an object for the given class and initialize it from conf
     *
     * @param theClass class of which an object is created
     * @return a new object
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> theClass) {
        T result;
        try {
            Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
            if (meth == null) {
                meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
                meth.setAccessible(true);
                CONSTRUCTOR_CACHE.put(theClass, meth);
            }
            result = meth.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

}
