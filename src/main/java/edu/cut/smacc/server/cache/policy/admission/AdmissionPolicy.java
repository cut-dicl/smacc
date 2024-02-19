package edu.cut.smacc.server.cache.policy.admission;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.configuration.Configuration;
import edu.cut.smacc.configuration.ServerConfigurations;
import edu.cut.smacc.server.cache.policy.CachePolicy;
import edu.cut.smacc.utils.ReflectionUtils;

/**
 * Interface for admission policies
 */
public interface AdmissionPolicy extends CachePolicy {

    void initialize(Configuration conf);

    /**
     * Get the admission location for a file that is being read
     * @param file the file
     * @return the admission location
     */
    StoreOptionType getReadAdmissionLocation(CacheFile file);

    /**
     * Get the admission location for a file that is being written
     * @param file the file
     * @return the admission location
     */
    StoreOptionType getWriteAdmissionLocation(CacheFile file);

    static AdmissionPolicy getInstance(Configuration conf) {
        Class<? extends AdmissionPolicy> admissionPolicyClass = conf.getClass(
                ServerConfigurations.ADMISSION_POLICY_CLASS_KEY,
                ServerConfigurations.ADMISSION_POLICY_CLASS_DEFAULT,
                AdmissionPolicy.class);
        AdmissionPolicy admissionPolicy = ReflectionUtils.newInstance(admissionPolicyClass);
        admissionPolicy.initialize(conf);
        return admissionPolicy;
    }

}
