package edu.cut.smacc.utils.collections;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class keeps a sorted tree based on a WeightedNode<U>'s weight.
 */
public class SortedWeightedTree<N extends WeightedNode<I>, I extends Comparable<? super I>> {

    private final TreeSet<N> weightedTree;
    private final Map<I, N> nodesMap;

    public SortedWeightedTree() {
        this.weightedTree = new TreeSet<>();
        this.nodesMap = new ConcurrentHashMap<>();
    }

    public SortedWeightedTree(Comparator<? super N> comparator) {
        this.weightedTree = new TreeSet<>(comparator);
        this.nodesMap = new HashMap<>();
    }

    public void addNode(N f) {
        if (!contains(f.item)) {
            nodesMap.put(f.item, f);
            weightedTree.add(f);
        }
    }

    public Iterator<N> descIter() {
        return new WeightedNodeIterator(weightedTree.descendingIterator(), nodesMap);
    }

    public Iterator<N> ascIter() {
        return new WeightedNodeIterator(weightedTree.iterator(), nodesMap);
    }

    public Iterator<I> descItemIter() {
        return new WeightedItemIterator(weightedTree.descendingIterator());
    }

    public Iterator<I> ascItemIter() {
        return new WeightedItemIterator(weightedTree.iterator());
    }

    public boolean contains(I item) {
        return nodesMap.containsKey(item);
    }

    public N getNode(I item) {
        return nodesMap.get(item);
    }

    public I getMinWeightItem() {
        if (!weightedTree.isEmpty())
            return weightedTree.first().item;
        else
            return null;
    }

    public I getMaxWeightItem() {
        if (!weightedTree.isEmpty())
            return weightedTree.last().item;
        else
            return null;
    }

    public void updateNode(I item) {
        if (item != null && nodesMap.containsKey(item)) {
            N changedNode = nodesMap.get(item);
            weightedTree.remove(changedNode);
            changedNode.updateWeight();
            weightedTree.add(changedNode);
        }
    }

    public N deleteNode(I item) {
        if (item != null && nodesMap.containsKey(item)) {
            weightedTree.remove(nodesMap.get(item));
            return nodesMap.remove(item);
        }

        return null;
    }

    public int size() {
        return weightedTree.size();
    }

    public void clear() {
        this.weightedTree.clear();
        this.nodesMap.clear();
    }

    @Override
    public String toString() {
        return weightedTree.toString();
    }

    /**
     * Item iterator
     */
    private class WeightedItemIterator implements Iterator<I> {

        Iterator<N> iter;

        public WeightedItemIterator(Iterator<N> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public I next() {
            return iter.next().item;
        }
    }

    /**
     * Weighted node iterator
     */
    private class WeightedNodeIterator implements Iterator<N> {

        Iterator<N> iter;
        Map<I, N> itemsMap;
        N curr;

        public WeightedNodeIterator(Iterator<N> iter, Map<I, N> itemsMap) {
            this.iter = iter;
            this.itemsMap = itemsMap;
            this.curr = null;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public N next() {
            curr = iter.next();
            return curr;
        }

        @Override
        public void remove() {
            if (curr == null)
                throw new IllegalStateException();

            iter.remove();
            itemsMap.remove(curr.item);
            curr = null;
        }
    }

}

