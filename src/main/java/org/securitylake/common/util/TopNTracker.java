package org.securitylake.common.util;

import java.util.PriorityQueue;

/**
 * Top N style tracker
 *
 * @param <T>
 */
public class TopNTracker<T> extends PriorityQueue<TopNTracker.Metric<T>>
{
    private int n;
    private long total = 0;

    public TopNTracker(int n)
    {
        super(n);
        this.n = n;
    }

    @Override
    public boolean offer(Metric<T> entry)
    {
        updateTotal(entry.value);
        return addEntry(entry);

    }

    private boolean addEntry(Metric<T> entry)
    {
        if (size() == n)
        {
            Metric<T> smallest = peek();
            if (smallest.value >= entry.value)
            {
                return false;
            }
            // remove smallest
            poll();
        }
        return super.offer(entry);
    }

    /**
     * Merge supplied tracker to this tracker
     *
     * @param otherTracker
     */
    public void merge(TopNTracker<T> otherTracker)
    {
        updateTotal(otherTracker.total);
        otherTracker.stream().forEach(tm -> addEntry(tm));
    }

    private void updateTotal(long value)
    {
        total += value;
    }

    public long getTotal()
    {
        return total;
    }


    /**
     * Metric to track
     *
     * @param <T>
     */
    public static final class Metric<T> implements Comparable<Metric<T>>
    {
        private final T t;
        private final long value;

        public Metric(T t, long value)
        {
            this.t = t;
            this.value = value;
        }

        public T getT()
        {
            return t;
        }

        public long getValue()
        {
            return value;
        }

        @Override
        public int compareTo(Metric<T> o)
        {
            return (int) (value - o.value);
        }

        @Override
        public boolean equals(Object other)
        {
            if (other instanceof Metric)
            {
                return compareTo((Metric) other) == 0;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return Long.valueOf(value).hashCode();
        }

        @Override
        public String toString()
        {
            return "{" + "t=" + t + ", value=" + value + '}';
        }
    }
}
