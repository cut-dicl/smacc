package edu.cut.smacc.configuration;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;

public class Configuration extends PropertiesConfiguration {

    private static final Logger logger = LogManager.getLogger(Configuration.class);

    public Configuration(String path) throws ConfigurationException {
        loadFromFile(path);
    }

    public Configuration() {
        super();
    }

    /**
     * Get the value of the <code>key</code> property as a <code>Class</code>
     * implementing the interface specified by <code>xface</code>.
     * If no such property is specified, then <code>defaultValue</code> is
     * returned.
     *
     * @param <U>
     * @param key
     * @param defaultValue
     * @param xface
     * @return
     */
    public <U> Class<? extends U> getClass(String key,
                                           Class<? extends U> defaultValue,
                                           Class<U> xface) {
        try {
            Class<?> theClass = getClass(key, defaultValue);
            if (theClass != null && !xface.isAssignableFrom(theClass))
                throw new RuntimeException(theClass + " not " + xface.getName());
            else if (theClass != null)
                return theClass.asSubclass(xface);
            else
                return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the value of the <code>key</code> property as a <code>Class</code>.
     * If no such property is specified, then <code>defaultValue</code> is
     * returned.
     *
     * @param key
     * @param defaultValue
     * @return a class object
     */
    public Class<?> getClass(String key, Class<?> defaultValue) {
        String valueString = getString(key);
        if (valueString == null)
            return defaultValue;
        try {
            return Class.forName(valueString.trim());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadFromFile(String path) throws ConfigurationException {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(
                PropertiesConfiguration.class)
                .configure(params.properties()
                        .setFileName(path));

        FileBasedConfiguration config = null;
        try {
            config = builder.getConfiguration();
        } catch (org.apache.commons.configuration2.ex.ConfigurationException e) {
            throw new ConfigurationException(e.getMessage(), e.getCause());
        }

        // Remove duplicated values and leave only the last.
        Iterator<String> it = config.getKeys();
        while (it.hasNext()) {
            final String name = it.next();
            List<Object> values = config.getList(name);
            addProperty(name, values.get(values.size() - 1));
            logger.debug(name + " = " + getProperty(name));
        }
    }

}
