package edu.cut.smacc.utils.collections;

import java.util.Comparator;

/**
 * Weighted Node that keeps track both of frequency and recency data. To be used
 * with comparators for getting either LFU or LRU functionality. Will default to
 * LRU by itself.
 */
public class WeightedLIFENode<I extends Comparable<? super I>> extends WeightedNode<I> {

    private long numAccesses;
    private long lastAccessTime;

    public WeightedLIFENode(I f, long lastAccessTime) {
        super(f);
        this.numAccesses = 0;
        this.lastAccessTime = lastAccessTime;
    }

    public long getNumAccesses() {
        return numAccesses;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public void updateWeight() {
        this.numAccesses = this.numAccesses + 1;
        this.lastAccessTime = System.currentTimeMillis();
    }

    @Override
    public double getWeight() {
        return lastAccessTime; // Defaults to LRU
    }

    @Override
    public String toString() {
        return "WeightedLIFENode [file=" + this.getItem() + ", numAccesses="
                + numAccesses + ", lastAccess=" + lastAccessTime + "]";
    }

    /**
     * Custom LFU comparator
     */
    public static class LFUComparator<I extends Comparable<? super I>> implements Comparator<WeightedLIFENode<I>> {
        @Override
        public int compare(WeightedLIFENode<I> o1, WeightedLIFENode<I> o2) {
            if (o1.item.equals(o2.item))
                return 0;

            int comp = Long.compare(o1.numAccesses, o2.numAccesses);
            if (comp == 0)
                return o1.item.compareTo(o2.item);
            else
                return comp;
        }
    }

    /**
     * Custom LRU comparator
     */
    public static class LRUComparator<I extends Comparable<? super I>> implements Comparator<WeightedLIFENode<I>> {
        @Override
        public int compare(WeightedLIFENode<I> o1, WeightedLIFENode<I> o2) {
            if (o1.item.equals(o2.item))
                return 0;

            int comp = Long.compare(o1.lastAccessTime, o2.lastAccessTime);
            if (comp == 0)
                return o1.item.compareTo(o2.item);
            else
                return comp;
        }
    }

}
