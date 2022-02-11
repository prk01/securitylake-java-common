
package org.securitylake.common.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test
 */
public class SlidingWindowCounterTest
{
    @Test
    public void testErrorCountWithinWindow()
    {
        SlidingWindowCounter errorCount = new SlidingWindowCounter(5);
        Assert.assertEquals(errorCount.getCount(), 0);
        Assert.assertEquals(errorCount.incrementBy(100), 100);
        Assert.assertEquals(errorCount.getCount(), 100);
    }

    @Test(expectedExceptions = AssertionError.class)
    public void testWrongArguments()
    {
        new SlidingWindowCounter(-1);
    }

    @Test
    public void testErrorCountOutsideWindow()
    {
        SlidingWindowCounter errorCount = new SlidingWindowCounter(5);

        errorCount.incrementBy(100, System.currentTimeMillis() + 10 * 60 * 1000);
        Assert.assertEquals(getCount(errorCount.getCounters()), 100);
    }

    @Test
    public void testErrorCountWithOverlapWindow()
    {
        SlidingWindowCounter errorCount = new SlidingWindowCounter(5);
        errorCount.incrementBy(50);
        Assert.assertEquals(getCount(errorCount.getCounters()), 50);

        errorCount.incrementBy(100, System.currentTimeMillis() + 4 * 60 * 1000);
        Assert.assertEquals(getCount(errorCount.getCounters()), 150);

        errorCount.incrementBy(150, System.currentTimeMillis() + 8 * 60 * 1000);
        Assert.assertEquals(getCount(errorCount.getCounters()), 250);

        errorCount.incrementBy(200, System.currentTimeMillis() + 9 * 60 * 1000);
        Assert.assertEquals(getCount(errorCount.getCounters()), 350);

        errorCount.incrementBy(250, System.currentTimeMillis() + 10 * 60 * 1000);
        Assert.assertEquals(getCount(errorCount.getCounters()), 600);
    }

    private long getCount(long[] errorCounts)
    {
        long count = 0;
        for (long l : errorCounts)
        {
            count += l;
        }

        return count;
    }
}
