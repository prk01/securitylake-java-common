package org.securitylake.common.util;

import com.google.common.annotations.VisibleForTesting;

import java.time.Duration;
import java.util.Arrays;

/**
 * Sliding window based counter to provide the stats counted in last N minutes.
 */
public class SlidingWindowCounter
{
    private long[] counters;
    private int startIndex;
    private long windowStartMillis;
    private long windowEndMillis;
    private int windowLengthMinutes;

    private static final long ONE_MINUTE_MILLIS = Duration.ofMinutes(1).toMillis();

    /**
     * Constructor
     *
     * @param windowLengthMinutes - window length in minutes
     */
    public SlidingWindowCounter(int windowLengthMinutes)
    {
        assert windowLengthMinutes > 0 : "windowLengthMinutes must be > 0";
        this.windowLengthMinutes = windowLengthMinutes;
        counters = new long[windowLengthMinutes];
        initialise(System.currentTimeMillis());

    }

    /**
     * Increment the counter by supplied count and return the current count
     *
     * @param count
     * @return the current count
     */
    public synchronized long incrementBy(long count)
    {
        incrementBy(count, System.currentTimeMillis());
        long c = 0;
        for (long l : counters)
        {
            c += l;
        }

        return c;
    }

    /**
     * Get the current count
     *
     * @return
     */
    public synchronized long getCount()
    {
        incrementBy(0, System.currentTimeMillis());
        long count = 0;
        for (long l : counters)
        {
            count += l;
        }

        return count;
    }

    @VisibleForTesting
    long[] getCounters()
    {
        return counters;
    }

    @VisibleForTesting
    synchronized void incrementBy(long count, long currentMillis)
    {
        if (currentMillis < windowEndMillis)
        {
            int slot = findSlot(currentMillis, windowStartMillis);
            counters[getIndex(slot)] += count;
        } else if (currentMillis >= (windowEndMillis + ONE_MINUTE_MILLIS * windowLengthMinutes))
        {
            initialise(currentMillis);
            counters[0] = count;
        } else
        {
            int slot = findSlot(currentMillis, windowEndMillis);

            // init slot places by advancing startIndex
            for (int index = 0; index < slot; index++)
            {
                startIndex = getIndex(index);
                counters[startIndex] = 0;
            }
            //last slot
            counters[getIndex(counters.length)] = count;

            //next slot is our startIndex
            startIndex = getIndex(1);
            windowEndMillis = currentMillis;
            windowStartMillis = windowEndMillis - ONE_MINUTE_MILLIS * windowLengthMinutes;

        }
    }

    private int getIndex(int slot)
    {
        return (startIndex + slot) % counters.length;
    }

    private int findSlot(long currentMillis, long start)
    {
        return (int) ((currentMillis - start) / (ONE_MINUTE_MILLIS));
    }

    private void initialise(long startWindow)
    {
        Arrays.fill(counters, 0);
        startIndex = 0;
        windowStartMillis = startWindow;
        windowEndMillis = windowStartMillis + ONE_MINUTE_MILLIS * windowLengthMinutes;
    }
}
