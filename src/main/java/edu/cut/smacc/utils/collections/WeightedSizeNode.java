package edu.cut.smacc.utils.collections;

/**
 * Weighted Node based on a size
 */
public class WeightedSizeNode<I extends Comparable<? super I>> extends WeightedNode<I> {

    private long size;

    public WeightedSizeNode(I f, long size) {
        super(f);
        this.size = size;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long fileSize) {
        this.size = fileSize;
    }

    @Override
    public void updateWeight() {
        // nothing to do
    }

    @Override
    public double getWeight() {
        return size;
    }

    @Override
    public String toString() {
        return "WeightedFileSizeNode [file=" + this.getItem() + ", file size=" + size + "]";
    }

}
