package edu.cut.smacc.utils.collections;

public class WeightedLRFUNode<I extends Comparable<? super I>> extends WeightedNode<I> {

    // Represents "half life", i.e., after how much time the weight is halved
    public static double BIAS = 1d * 60 * 60 * 1000;

    private double weight;
    private long lastAccess;

    public WeightedLRFUNode(I f, long lastAccess) {
        super(f);
        this.lastAccess = lastAccess;
        this.weight = BIAS / (0.001d * (System.currentTimeMillis() - this.lastAccess) + BIAS);
        if (this.weight > 0.99999d)
            this.weight = 1d; // this is a new file
    }

    public WeightedLRFUNode(I f, WeightedLRFUNode<I> otherNode) {
        super(f);
        this.lastAccess = otherNode.lastAccess;
        this.weight = otherNode.weight;
    }

    @Override
    public int compareTo(WeightedNode<I> o1) {
        if (this.item.equals(o1.item))
            return 0;

        WeightedLRFUNode<I> other = (WeightedLRFUNode<I>) o1;
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
        this.weight = 1 + BIAS * this.weight / (0.001d * (now - this.lastAccess) + BIAS);
        this.lastAccess = now;
    }

    @Override
    public double getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return "WeightedLRFUNode [file=" + this.getItem() + ", weight=" + weight + "]";
    }

}

