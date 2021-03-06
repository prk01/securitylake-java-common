package org.securitylake.common.util;

import com.google.common.util.concurrent.*;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This ExecutorService blocks the submission of new tasks when its queue is already full by using a semaphore.
 * <p>
 * Task submissions require permits, task completions release permits.
 * <p>
 */
public class BlockingThreadPoolExecutorService extends ForwardingListeningExecutorService
{
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BlockingThreadPoolExecutorService.class);
    private static final AtomicInteger POOLNUMBER = new AtomicInteger(1);

    private Semaphore queueingPermits;
    private ListeningExecutorService executorDelegatee;

    /**
     * A thread pool that that blocks clients submitting additional tasks if
     * there are already {@code activeTasks} running threads and {@code
     * waitingTasks} tasks waiting in its queue.
     *
     * @param activeTasks   maximum number of active tasks
     * @param waitingTasks  maximum number of waiting tasks
     * @param keepAliveTime time until threads are cleaned up in {@code unit}
     * @param unit          time unit
     * @param prefixName    prefix of name for threads
     */
    public BlockingThreadPoolExecutorService(int activeTasks, int waitingTasks, long keepAliveTime, TimeUnit unit, String prefixName)
    {
        super();
        queueingPermits = new Semaphore(waitingTasks + activeTasks, false);
        // Although we generally only expect up to waitingTasks tasks in the queue, we need to be able to buffer all tasks in case dequeueing is slower than enqueueing.
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(waitingTasks + activeTasks);
        ThreadPoolExecutor eventProcessingExecutor = new ThreadPoolExecutor(activeTasks, activeTasks, keepAliveTime, unit, workQueue, newDaemonThreadFactory(prefixName),
                (r, executor) ->
                {
                    // This is not expected to happen.
                    log.warn("Could not submit task to executor {}", executor.toString());
                });
        eventProcessingExecutor.allowCoreThreadTimeOut(true);
        executorDelegatee = MoreExecutors.listeningDecorator(eventProcessingExecutor);
    }

    /**
     * Returns a {@link java.util.concurrent.ThreadFactory} that names each
     * created thread uniquely,
     * with a common prefix.
     *
     * @param prefix The prefix of every created Thread's name
     * @return a {@link java.util.concurrent.ThreadFactory} that names threads
     */
    public static ThreadFactory getNamedThreadFactory(final String prefix)
    {
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup threadGroup = (s != null) ? s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();

        return new ThreadFactory()
        {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            private final int poolNum = POOLNUMBER.getAndIncrement();
            private final ThreadGroup group = threadGroup;

            @Override
            public Thread newThread(Runnable r)
            {
                final String name =
                        prefix + "-pool" + poolNum + "-t" + threadNumber.getAndIncrement();
                return new Thread(group, r, name);
            }
        };
    }

    /**
     * Get a named {@link ThreadFactory} that just builds daemon threads.
     *
     * @param prefix name prefix for all threads created from the factory
     * @return a thread factory that creates named, daemon threads with
     * the supplied exception handler and normal priority
     */
    private static ThreadFactory newDaemonThreadFactory(final String prefix)
    {
        final ThreadFactory namedFactory = getNamedThreadFactory(prefix);
        return r ->
        {
            Thread t = namedFactory.newThread(r);
            if (!t.isDaemon())
            {
                t.setDaemon(true);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY)
            {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        };
    }

    @Override
    protected ListeningExecutorService delegate()
    {
        return executorDelegatee;
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task)
    {
        try
        {
            queueingPermits.acquire();
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }
        return super.submit(new CallableWithPermitRelease<T>(task));
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result)
    {
        try
        {
            queueingPermits.acquire();
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }
        return super.submit(new RunnableWithPermitRelease(task), result);
    }

    @Override
    public ListenableFuture<?> submit(Runnable task)
    {
        try
        {
            queueingPermits.acquire();
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }
        return super.submit(new RunnableWithPermitRelease(task));
    }

    @Override
    public void execute(Runnable command)
    {
        try
        {
            queueingPermits.acquire();
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        super.execute(new RunnableWithPermitRelease(command));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
    {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
    {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Returns queue length.
     *
     * @return queueLength
     */
    public int getQueueLength()
    {
        return queueingPermits.getQueueLength();
    }

    /**
     * Releases a permit after the task is executed.
     */
    class RunnableWithPermitRelease implements Runnable
    {
        private Runnable delegatee;

        public RunnableWithPermitRelease(Runnable delegatee)
        {
            this.delegatee = delegatee;
        }

        @Override
        public void run()
        {
            try
            {
                delegatee.run();
            } finally
            {
                queueingPermits.release();
            }

        }
    }

    /**
     * Releases a permit after the task is completed.
     */
    class CallableWithPermitRelease<T> implements Callable<T>
    {
        private Callable<T> delegatee;

        public CallableWithPermitRelease(Callable<T> delegatee)
        {
            this.delegatee = delegatee;
        }

        @Override
        public T call() throws Exception
        {
            try
            {
                return delegatee.call();
            } finally
            {
                queueingPermits.release();
            }
        }

    }

}
