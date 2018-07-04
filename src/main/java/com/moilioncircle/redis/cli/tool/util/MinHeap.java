package com.moilioncircle.redis.cli.tool.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Baoyi Chen
 */
public class MinHeap<T extends Comparable<T>> {

    private final int n;
    private final List<T> ary;
    private Consumer<T> consumer;

    public void setConsumer(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    public MinHeap(int n) {
        this.n = n;
        this.ary = new ArrayList<>();
    }

    @SuppressWarnings("unused")
    private int parent(int i) {
        if (i == 0)
            return -1;
        return i / 2;
    }

    private int left(int i) {
        if (i == 0)
            return 1;
        return 2 * i;
    }

    private int right(int i) {
        if (i == 0)
            return 2;
        return 2 * i + 1;
    }

    private void minHeapify(List<T> ary, int i) {
        int l = left(i);
        int r = right(i);
        int min = i;
        if (l < ary.size()) {
            if (ary.get(l).compareTo(ary.get(i)) < 0) {
                min = l;
            }
        }
        if (r < ary.size()) {
            if (ary.get(r).compareTo(ary.get(min)) < 0) {
                min = r;
            }
        }
        if (min != i) {
            T temp = ary.get(i);
            ary.set(i, ary.get(min));
            ary.set(min, temp);
            minHeapify(ary, min);
        }
    }

    private void builtMinHeap(List<T> a) {
        for (int i = (a.size() - 1) / 2; i >= 0; i--) {
            minHeapify(a, i);
        }
    }

    public void add(T t) {
        if (n <= 0) {
            if (consumer != null) consumer.accept(t);
            return;
        }
        if (ary.size() < n) {
            ary.add(t);
            return;
        }
        if (ary.size() == n) {
            builtMinHeap(ary);
        }
        if (ary.get(0).compareTo(t) < 0) {
            ary.set(0, t);
            minHeapify(ary, 0);
        }
    }

    public List<T> get() {
        Collections.sort(ary, Comparator.reverseOrder());
        return ary;
    }
}
