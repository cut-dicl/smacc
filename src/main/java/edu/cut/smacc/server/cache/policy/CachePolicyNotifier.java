package edu.cut.smacc.server.cache.policy;

import edu.cut.smacc.server.cache.common.CacheFile;
import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.cache.policy.eviction.item.EvictionItemPolicy;
import edu.cut.smacc.server.cache.policy.eviction.placement.EvictionPlacementPolicy;

import java.sql.Timestamp;
import java.util.List;

/**
 * Helper class that notifies the given cache policies for cache interactions.
 */
public class CachePolicyNotifier {

    private final List<CachePolicy> cachePolicies;

    private CachePolicyNotifier(List<CachePolicy> cachePolicies) {
        this.cachePolicies = cachePolicies;
    }

    public static CachePolicyNotifier createNotifierFromPoliciesList(List<CachePolicy> cachePolicies) {
        return new CachePolicyNotifier(cachePolicies);
    }

    public List<CachePolicy> getCachePolicies() {
        return cachePolicies;
    }

    public EvictionItemPolicy getEvictionItemPolicy() {
        for (CachePolicy cachePolicy : cachePolicies) {
            if (cachePolicy instanceof EvictionItemPolicy) {
                return (EvictionItemPolicy) cachePolicy;
            }
        }
        return null;
    }

    public EvictionPlacementPolicy getEvictionPlacementPolicy() {
        for (CachePolicy cachePolicy : cachePolicies) {
            if (cachePolicy instanceof EvictionPlacementPolicy) {
                return (EvictionPlacementPolicy) cachePolicy;
            }
        }
        return null;
    }

    public void notifyItemAddition(CacheFile file, StoreOptionType tier) {
    	System.out.println(new Timestamp(System.currentTimeMillis())
				+ ":: [SMACC] onItemAdd: "+file.getKey()+" in "+tier.toString());
        for (CachePolicy cachePolicy : cachePolicies) {
            cachePolicy.onItemAdd(file, tier);
        }
    }

    public void notifyItemAccess(CacheFile file, StoreOptionType tier) {
    	System.out.println(new Timestamp(System.currentTimeMillis())
				+ ":: [SMACC] onItemAccess: "+file.getKey()+" tier= "+tier.toString());
        for (CachePolicy cachePolicy : cachePolicies) {
            cachePolicy.onItemAccess(file, tier);
        }
    }

    public void notifyItemUpdate(CacheFile file, StoreOptionType tier) {
    	System.out.println(new Timestamp(System.currentTimeMillis())
				+ ":: [SMACC] onItemUpdate: "+file.getKey()+" tier= "+tier.toString());
        for (CachePolicy cachePolicy : cachePolicies) {
            cachePolicy.onItemUpdate(file, tier);
        }
    }

    public void notifyItemDeletion(CacheFile file, StoreOptionType tier) {
    	System.out.println(new Timestamp(System.currentTimeMillis())
				+ ":: [SMACC] onItemDelete: "+file.getKey()+" from "+tier.toString());
        for (CachePolicy cachePolicy : cachePolicies) {
            cachePolicy.onItemDelete(file, tier);
        }
    }

    public void notifyItemNotAdded(CacheFile file, StoreOptionType tier) {
    	System.out.println(new Timestamp(System.currentTimeMillis())
				+ ":: [SMACC] onItemNotAdded: "+file.getKey()+" tier= "+tier.toString());
        for (CachePolicy cachePolicy : cachePolicies) {
            cachePolicy.onItemNotAdded(file, tier);
        }
    }

    public void notifyPolicyReset() {
        for (CachePolicy cachePolicy : cachePolicies) {
            cachePolicy.reset();
        }
    }

}
