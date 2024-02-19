package edu.cut.smacc.utils.collections;

/**
 * LFU Node based on number of accesses
 */
public class WeightedLFUNode<I extends Comparable<? super I>> extends WeightedNode<I> {
    private long numAccesses;

    public WeightedLFUNode(I item) {
        super(item);
        this.numAccesses = 0;
    }

    public WeightedLFUNode(I item, long numAccess) {
        super(item);
        this.numAccesses = numAccess;
    }

    @Override
    public void updateWeight() {
        this.numAccesses = this.numAccesses + 1;
    }

    @Override
    public double getWeight() {
        return numAccesses;
    }

    @Override
    public String toString() {
        return "WeightedLFUNode [item=" + getItem() + ", access=" + numAccesses + "]";
    }

}