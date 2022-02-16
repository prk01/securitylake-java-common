package org.securitylake.common.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * TopNTrackerTest
 */
public class TopNTrackerTest
{
    @Test
    public void testTopN()
    {
        TopNTracker<String> topNTracker = new TopNTracker<>(3);

        topNTracker.add(new TopNTracker.Metric<>("val10", 200));
        topNTracker.add(new TopNTracker.Metric<>("val50", 300));
        topNTracker.add(new TopNTracker.Metric<>("val100", 100));
        topNTracker.add(new TopNTracker.Metric<>("val250", 500));

        assertEquals(topNTracker.getTotal(), 1100);

        assertEquals(topNTracker.size(), 3);
        TopNTracker.Metric<String> metric = topNTracker.poll();
        assertEquals(metric.getT(), "val10");
        assertEquals(metric.getValue(), 200);
        assertEquals(topNTracker.poll().getT(), "val50");
        assertEquals(topNTracker.poll().getT(), "val250");


    }

    @Test
    public void testTopNWhenEquals()
    {
        TopNTracker<String> topNTracker = new TopNTracker<>(3);

        topNTracker.add(new TopNTracker.Metric<>("val10", 200));
        topNTracker.add(new TopNTracker.Metric<>("val50", 300));
        topNTracker.add(new TopNTracker.Metric<>("val100", 100));
        topNTracker.add(new TopNTracker.Metric<>("val250", 100));

        assertEquals(topNTracker.getTotal(), 700);

        assertEquals(topNTracker.size(), 3);
        TopNTracker.Metric<String> metric = topNTracker.poll();
        assertEquals(metric.getT(), "val100");
        assertEquals(metric.getValue(), 100);
        assertEquals(topNTracker.poll().getT(), "val10");
        assertEquals(topNTracker.poll().getT(), "val50");

    }

    @Test
    public void testMerge()
    {
        TopNTracker<String> topNTracker = new TopNTracker<>(3);

        topNTracker.add(new TopNTracker.Metric<>("val10", 200));
        topNTracker.add(new TopNTracker.Metric<>("val50", 300));
        topNTracker.add(new TopNTracker.Metric<>("val100", 100));
        topNTracker.add(new TopNTracker.Metric<>("val250", 100));

        assertEquals(topNTracker.getTotal(), 700);

        TopNTracker<String> otherTracker = new TopNTracker<>(3);

        topNTracker.add(new TopNTracker.Metric<>("v1", 500));
        topNTracker.add(new TopNTracker.Metric<>("v2", 300));

        topNTracker.merge(otherTracker);

        assertEquals(topNTracker.size(), 3);
        assertEquals(topNTracker.getTotal(), 1500);
        TopNTracker.Metric<String> metric = topNTracker.poll();
        assertEquals(metric.getT(), "val50");
        assertEquals(metric.getValue(), 300);
        assertEquals(topNTracker.poll().getT(), "v2");
        assertEquals(topNTracker.poll().getT(), "v1");

    }

    @Test
    public void testMetric()
    {
        TopNTracker.Metric m1 = new TopNTracker.Metric<>("val10", 200);
        TopNTracker.Metric m2 = new TopNTracker.Metric<>("val50", 200);

        assertEquals(m1, m2);
        assertEquals(m1.toString(), "{t=val10, value=200}");
    }
}

