package edu.cut.smacc.utils.collections;

/**
 * Base class for a weighted node
 */
public abstract class WeightedNode<I extends Comparable<? super I>> implements Comparable<WeightedNode<I>> {

    protected I item;

    public WeightedNode(I f) {
        this.item = f;
    }

    @Override
    public int compareTo(WeightedNode<I> o1) {
        if (item.equals(o1.item))
            return 0;

        int comp = Double.compare(this.getWeight(), o1.getWeight());
        if (comp == 0)
            return item.compareTo(o1.item);
        else
            return comp;
    }

    public I getItem() {
        return item;
    }

    public abstract double getWeight();

    public abstract void updateWeight();
}

