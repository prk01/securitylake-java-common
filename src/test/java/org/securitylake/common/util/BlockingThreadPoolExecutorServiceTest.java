package org.securitylake.common.util;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Basic unit test for blocking executor service.
 */
public class BlockingThreadPoolExecutorServiceTest
{
    private static final Logger LOG = LoggerFactory.getLogger(BlockingThreadPoolExecutorService.class);

    private static final int NUM_ACTIVE_TASKS = 4;
    private static final int NUM_WAITING_TASKS = 2;
    private static final int TASK_SLEEP_MSEC = 100;
    private static final int SHUTDOWN_WAIT_MSEC = 200;
    private static final int SHUTDOWN_WAIT_TRIES = 5;
    private static final int BLOCKING_THRESHOLD_MSEC = 50;

    private static final Integer SOME_VALUE = 1337;

    private static volatile BlockingThreadPoolExecutorService tpe = null;

    private Runnable sleeper = () ->
    {
        String name = Thread.currentThread().getName();
        try
        {
            Thread.sleep(TASK_SLEEP_MSEC);
        } catch (InterruptedException e)
        {
            LOG.info("Thread {} interrupted.", name);
            Thread.currentThread().interrupt();
        }
    };

    private Callable<Integer> callableSleeper = () ->
    {
        sleeper.run();
        return SOME_VALUE;
    };

    @AfterTest
    public static void afterClass() throws Exception
    {
        ensureDestroyed();
    }

    /**
     * Helper function to create thread pool under test.
     */
    private static void ensureCreated() throws Exception
    {
        if (tpe == null)
        {
            LOG.debug("Creating thread pool");
            tpe = new BlockingThreadPoolExecutorService(NUM_ACTIVE_TASKS,
                    NUM_WAITING_TASKS, 1, TimeUnit.SECONDS, "btpetest");
        }
    }

    // Helper functions, etc.

    /**
     * Helper function to terminate thread pool under test, asserting that
     * shutdown -> terminate works as expected.
     */
    private static void ensureDestroyed() throws Exception
    {
        if (tpe == null)
        {
            return;
        }
        int shutdownTries = SHUTDOWN_WAIT_TRIES;

        tpe.shutdown();
        if (!tpe.isShutdown())
        {
            throw new RuntimeException("Shutdown had no effect.");
        }

        while (!tpe.awaitTermination(SHUTDOWN_WAIT_MSEC,
                TimeUnit.MILLISECONDS))
        {
            LOG.info("Waiting for thread pool shutdown.");
            if (shutdownTries-- <= 0)
            {
                LOG.error("Failed to terminate thread pool gracefully.");
                break;
            }
        }
        if (!tpe.isTerminated())
        {
            tpe.shutdownNow();
            if (!tpe.awaitTermination(SHUTDOWN_WAIT_MSEC,
                    TimeUnit.MILLISECONDS))
            {
                throw new RuntimeException(
                        "Failed to terminate thread pool in timely manner.");
            }
        }
        tpe = null;
    }

    /**
     * Basic test of running one trivial task.
     */
    @Test
    public void testSubmitCallable() throws Exception
    {
        ensureCreated();
        ListenableFuture<Integer> f = tpe.submit(callableSleeper);
        Integer v = f.get();
        assertEquals(SOME_VALUE, v);
    }

    @Test
    public void testExecute() throws Exception
    {
        ensureCreated();
        int totalTasks = NUM_ACTIVE_TASKS + NUM_WAITING_TASKS;
        Stopwatch stopWatch = Stopwatch.createStarted();
        for (int i = 0; i < totalTasks; i++)
        {
            tpe.execute(sleeper);
            assertDidntBlock(stopWatch);
        }
        tpe.submit(sleeper);
        assertDidBlock(stopWatch);
    }

    /**
     * More involved test, including detecting blocking when at capacity.
     */
    @Test
    public void testSubmitRunnable() throws Exception
    {
        ensureCreated();
        int totalTasks = NUM_ACTIVE_TASKS + NUM_WAITING_TASKS;
        Stopwatch stopWatch = Stopwatch.createStarted();
        for (int i = 0; i < totalTasks; i++)
        {
            tpe.submit(sleeper);
            assertDidntBlock(stopWatch);
        }
        tpe.submit(sleeper);
        assertDidBlock(stopWatch);
    }

    @Test
    public void testSubmitRunnableResult() throws Exception
    {
        ensureCreated();
        int totalTasks = NUM_ACTIVE_TASKS + NUM_WAITING_TASKS;
        Stopwatch stopWatch = Stopwatch.createStarted();
        for (int i = 0; i < totalTasks; i++)
        {
            ListenableFuture<Integer> submit = tpe.submit(sleeper, 1);
            assertEquals(submit.get(), Integer.valueOf(1));
        }
        tpe.submit(sleeper, 1);
        assertDidBlock(stopWatch);
    }

    /**
     * Test specifically that it works with 0 waiting tasks, to support BaseS3FileUploader
     */
    @Test
    public void testSubmitRunnableWithZeroWaiting() throws Exception
    {
        int numActiveTasks = 1;
        int numWaitingTasks = 0;
        BlockingThreadPoolExecutorService pool = new BlockingThreadPoolExecutorService(numActiveTasks,
                numWaitingTasks, 1, TimeUnit.SECONDS, "btpetest");

        int totalTasks = numActiveTasks + numWaitingTasks;
        Stopwatch stopWatch = Stopwatch.createStarted();
        for (int i = 0; i < totalTasks; i++)
        {
            pool.submit(sleeper);
            assertDidntBlock(stopWatch);
        }
        pool.submit(sleeper);
        assertDidBlock(stopWatch);
    }

    @Test
    public void testShutdown() throws Exception
    {
        // Cover create / destroy, regardless of when this test case runs
        ensureCreated();
        ensureDestroyed();

        // Cover create, execute, destroy, regardless of when test case runs
        ensureCreated();
        testSubmitRunnable();
        ensureDestroyed();
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testInvokeAll() throws InterruptedException
    {
        tpe.invokeAll(Collections.emptyList());
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testInvokeAll2() throws InterruptedException
    {
        tpe.invokeAll(Collections.emptyList(), 1, TimeUnit.SECONDS);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testInvokeAaby() throws InterruptedException, TimeoutException, ExecutionException
    {
        tpe.invokeAny(Collections.emptyList());
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testInvokeAaby2() throws InterruptedException, TimeoutException, ExecutionException
    {
        tpe.invokeAny(Collections.emptyList(), 1, TimeUnit.SECONDS);
    }

    private void assertDidntBlock(Stopwatch sw)
    {
        assertDidntBlock(sw, BLOCKING_THRESHOLD_MSEC);
    }

    private void assertDidntBlock(Stopwatch sw, long blockingThreasholdMsec)
    {
        try
        {
            long time = sw.elapsed(TimeUnit.MILLISECONDS) - blockingThreasholdMsec;
            assertFalse("Non-blocking call took too long - " + time, time > 0);
        } finally
        {
            sw.reset();
            sw.start();
        }
    }

    private void assertDidBlock(Stopwatch sw)
    {
        try
        {
            if (sw.elapsed(TimeUnit.MILLISECONDS) < BLOCKING_THRESHOLD_MSEC)
            {
                throw new RuntimeException("Blocking call returned too fast.");
            }
        } finally
        {
            sw.reset();
            sw.start();
        }
    }

}

