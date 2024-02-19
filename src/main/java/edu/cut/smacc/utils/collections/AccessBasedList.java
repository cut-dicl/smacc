package edu.cut.smacc.utils.collections;

import edu.cut.smacc.server.cache.common.CacheFile;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class AccessBasedList<T extends CacheFile> {

    private ConcurrentDoublyLinkedList itemList;
    private Map<T, Node> itemMap;

    public AccessBasedList() {
        itemList = new ConcurrentDoublyLinkedList();
        itemMap = new ConcurrentHashMap<>();
    }

    public void add(T item) {
        if (item != null && !itemMap.containsKey(item)) {
            Node node = new Node(item);
            itemList.addToHead(node);
            itemMap.put(item, node);
        }
    }

    public void addLast(T item) {
        if (item != null && !itemMap.containsKey(item)) {
            Node node = new Node(item);
            node = itemList.addToTail(node);
            itemMap.put(item, node);
        }
    }

    public void addToNext(T current, T item) {
        if (current != null && itemMap.containsKey(current)
                && item != null && !itemMap.containsKey(item)) {
            Node currNode = itemMap.get(current);
            Node newNode = new Node(item);
            itemList.addToNext(currNode, newNode);
            itemMap.put(item, newNode);
        }
    }

    public void addNode(Node node) {
        if (!itemMap.containsKey(node.item)) {
            node = itemList.addToHead(node);
            itemMap.put(node.item, node);
        }
    }

    public void addNodeLast(Node node) {
        if (!itemMap.containsKey(node.item)) {
            node = itemList.addToTail(node);
            itemMap.put(node.item, node);
        }
    }

    public void accessItem(T item) {
        if (item == null) return;
        if (itemMap.containsKey(item)) {
            Node node = itemMap.get(item);
            itemList.moveNodeToHead(node);
        }
    }

    public Node deleteItem(T item) {
        if (item != null && itemMap.containsKey(item)) {
            itemList.removeNode(itemMap.get(item));
            Node node = itemMap.remove(item);
            node.clear();
            return node;
        }

        return null;
    }

    public boolean containsItem(T item) {
        return item != null && itemMap.containsKey(item);
    }

    public void replaceItem(T item, T[] items) {
        if (!containsItem(item) || items == null)
            return;

        boolean sameItem = false;
        for (int i = items.length - 1; i >= 0; --i) {
            // Add each new item next to the current item
            addToNext(item, items[i]);
            if (item.equals(items[i]))
                sameItem = true;
        }

        // Delete the item only if it wasn't part of the items list
        if (sameItem == false)
            deleteItem(item);
    }

    public Node removeLRUNode() {
        Node tail = itemList.removeTail();
        if (tail != null) {
            Node node = itemMap.remove(tail.item);
            node.clear();
            return node;
        } else
            return null;
    }

    public Node removeMRUNode() {
        Node head = itemList.removeHead();
        if (head != null) {
            Node node = itemMap.remove(head.item);
            node.clear();
            return node;
        } else
            return null;
    }

    public void printListState() {
        itemList.printList();
        System.out.println();
    }

    public T getLRUItem() {
        return itemList.getLRUItem();
    }

    public Iterator<T> getLRUItemIterator() {
        return itemList.getLRUItemIterator();
    }

    public ArrayList<T> getLRUItems(int count) {
        return itemList.getCountLRUItems(count);
    }

    public T getMRUItem() {
        return itemList.getMRUItem();
    }

    public Iterator<T> getMRUItemIterator() {
        return itemList.getMRUItemIterator();
    }

    public ArrayList<T> getMRUItems(int count) {
        return itemList.getCountMRUItems(count);
    }

    public int size() {
        return itemList.size();
    }

    public void clear() {
        while (removeLRUNode() != null)
            ;
    }

    @Override
    public String toString() {
        return itemList.toString();
    }

    /**
     * A custom double linked list implementation
     *
     * @author herodotos.herodotou
     */
    private class ConcurrentDoublyLinkedList {

        private int currSize;
        private Node head;
        private Node tail;

        public ConcurrentDoublyLinkedList() {
            currSize = 0;
            this.head = null;
            this.tail = null;
        }

        public synchronized T getLRUItem() {
            if (tail != null)
                return tail.getItem();
            else
                return null;
        }

        public synchronized T getMRUItem() {
            if (head != null)
                return head.getItem();
            else
                return null;
        }

        public synchronized Iterator<T> getLRUItemIterator() {
            return new DoublyLinkedListLRUIterator(tail);
        }

        public synchronized Iterator<T> getMRUItemIterator() {
            return new DoublyLinkedListMRUIterator(head);
        }

        public synchronized ArrayList<T> getCountLRUItems(int count) {
            ArrayList<T> itemsLRU = new ArrayList<T>(
                    (currSize < count) ? currSize : count);
            Node tmp = tail;
            int c = 0;
            while (tmp != null && c < count) {
                itemsLRU.add(tmp.getItem());
                tmp = tmp.getPrev();
                c++;
            }
            return itemsLRU;
        }

        public synchronized ArrayList<T> getCountMRUItems(int count) {
            ArrayList<T> itemsMRU = new ArrayList<T>(
                    (currSize < count) ? currSize : count);
            Node tmp = head;
            int c = 0;
            while (tmp != null && c < count) {
                itemsMRU.add(tmp.getItem());
                tmp = tmp.getNext();
                c++;
            }
            return itemsMRU;
        }

        public synchronized void removeNode(Node node) {
            if (head == null) {
                return;
            }

            Node prev = node.getPrev();
            Node next = node.getNext();

            if (prev != null) {
                prev.setNext(next);
            } else { // is the head
                head = next;
            }
            if (next != null) {
                next.setPrev(prev);
            } else { // is the tail
                tail = prev;
            }

            --currSize;
        }

        public synchronized Node removeHead() {
            if (head == null)
                return null;

            Node toReturn = head;
            removeNode(head);
            return toReturn;
        }

        public synchronized Node removeTail() {
            if (tail == null)
                return null;

            Node toReturn = tail;
            removeNode(tail);
            return toReturn;
        }

        public synchronized void printList() {
            if (head == null) {
                return;
            }
            Node tmp = head;
            while (tmp != null) {
                System.out.print(tmp);
                System.out.print(" ");
                tmp = tmp.getNext();
            }
            System.out.println();
        }

        @Override
        public synchronized String toString() {
            if (head == null) {
                return "[]";
            }

            StringBuilder sb = new StringBuilder("[T--");
            Node curr = tail;
            while (curr != null) {
                sb.append(curr.toString());
                sb.append("--");
                curr = curr.getPrev();
            }
            sb.append("H]");

            return sb.toString();
        }

        public synchronized Node addToHead(Node node) {
            if (head == null) {
                head = node;
                tail = node;
                currSize = 1;
                return node;
            }
            currSize++;
            node.setNext(head);
            head.setPrev(node);
            head = node;
            return node;
        }

        public synchronized Node addToTail(Node node) {
            if (head == null) {
                head = node;
                tail = node;
                currSize = 1;
                return node;
            }
            currSize++;
            node.setPrev(tail);
            tail.setNext(node);
            tail = node;
            return node;
        }

        public synchronized void addToNext(Node current, Node node) {
            if (current == null || node == null) {
                return;
            }

            if (current == head) {
                addToHead(node);
                return;
            }

            Node prev = current.getPrev();
            current.setPrev(node);
            node.setNext(current);
            node.setPrev(prev);
            prev.setNext(node);
            currSize++;
        }

        public synchronized void moveNodeToHead(Node node) {
            if (node == null || node == head) {
                return;
            }

            if (node == tail) {
                tail = tail.getPrev();
                tail.setNext(null);
            }

            Node prev = node.getPrev();
            Node next = node.getNext();
            prev.setNext(next);

            if (next != null) {
                next.setPrev(prev);
            }

            node.setPrev(null);
            node.setNext(head);
            head.setPrev(node);
            head = node;
        }

        public synchronized int size() {
            return currSize;
        }
    }

    /**
     * An iterator to iterate the list from LRU to MRU
     *
     * @author herodotos.herodotou
     */
    private class DoublyLinkedListLRUIterator implements Iterator<T> {

        private Node currNode;

        DoublyLinkedListLRUIterator(Node tail) {
            this.currNode = tail;
        }

        @Override
        public boolean hasNext() {
            return (currNode != null);
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException();

            T item = currNode.item;
            currNode = currNode.getPrev();
            return item;
        }
    }

    /**
     * An iterator to iterate the list from MRU to LRU
     *
     * @author herodotos.herodotou
     */
    private class DoublyLinkedListMRUIterator implements Iterator<T> {

        private Node currNode;

        DoublyLinkedListMRUIterator(Node head) {
            this.currNode = head;
        }

        @Override
        public boolean hasNext() {
            return (currNode != null);
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException();

            T node = currNode.item;
            currNode = currNode.getNext();
            return node;
        }
    }

    /**
     * An internal linked-list node
     *
     * @author herodotos.herodotou
     */
    private class Node {

        private T item;
        private Node prev;
        private Node next;

        public Node(T f) {
            this.item = f;
            this.prev = null;
            this.next = null;
        }

        public T getItem() {
            return item;
        }

        public void clear() {
            prev = null;
            next = null;
        }

        public Node getPrev() {
            return prev;
        }

        public void setPrev(Node prev) {
            this.prev = prev;
        }

        public Node getNext() {
            return next;
        }

        public void setNext(Node next) {
            this.next = next;
        }

        @Override
        public String toString() {
            return item.toString();
        }

    }

}
