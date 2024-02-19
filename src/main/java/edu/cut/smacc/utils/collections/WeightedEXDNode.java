package edu.cut.smacc.utils.collections;

/**
 * Weighted node based on a geometric relation between number and time of accesses
 *
 * Weight = 1 + [e^(-alpha * (time_now - time_last_access)] * Weight
 */
public class WeightedEXDNode<I extends Comparable<? super I>> extends WeightedNode<I> {

    // Exposes tradeoff between recency and frequency.
    // Larger values give more emphasis on recency
    public static double ALPHA = 1d / (1d * 60 * 60 * 1000);

    private double weight;
    private long lastAccess;

    public WeightedEXDNode(I f, long lastAccess) {
        super(f);
        this.lastAccess = lastAccess;
        this.weight = Math.exp(-0.001d * ALPHA * (System.currentTimeMillis() - this.lastAccess));
        if (this.weight > 0.9999d)
            this.weight = 1d; // this is a new file
    }

    public WeightedEXDNode(I f, WeightedEXDNode<I> otherNode) {
        super(f);
        this.lastAccess = otherNode.lastAccess;
        this.weight = otherNode.weight;
    }

    @Override
    public int compareTo(WeightedNode<I> o1) {
        if (this.item.equals(o1.item))
            return 0;

        WeightedEXDNode<I> other = (WeightedEXDNode<I>) o1;
        int comp = Double.compare(this.weight, other.weight);
        if (comp == 0)
            comp = Long.compare(this.lastAccess, other.lastAccess);
        if (comp == 0)
            comp = this.item.compareTo(other.item);

        return comp;
    }

    @Override
    public void updateWeight() {
        long now = System.currentTimeMillis();
        this.weight = 1 + Math.exp(-0.001d * ALPHA * (now - this.lastAccess)) * this.weight;
        this.lastAccess = now;
    }

    @Override
    public double getWeight() {
        return weight;
    }

    public double calcUpdatedWeight() {
        long now = System.currentTimeMillis();
        return Math.exp(-0.001d * ALPHA * (now - this.lastAccess)) * this.weight;
    }

    @Override
    public String toString() {
        return "WeightedEXDNode [file=" + this.getItem() + ", weight=" + weight + "]";
    }

}
