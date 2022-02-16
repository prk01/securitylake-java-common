package org.securitylake.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Throttling logger factory that is designed to limit the logs within a configurable time window
 */
public class ThrottlingLoggerFactory
{
    private static final Logger log = LoggerFactory.getLogger(ThrottlingLoggerFactory.class);
    //map maintaining all the loggers
    private static final Map<Logger, ThrottlingLogger> LOGGER_MAP = new ConcurrentHashMap<>();
    private static final int DEFAULT_MESSAGE_COUNT_PER_LOGGER = 100;
    public static final int PERCENTAGE_TO_REDUCE = 5;
    public static final int MAX_OFFENDERS_TO_REDUCE = 5;
    public static final int MAX_CONSECUTIVE_OVERFLOW_TO_TRIGGER_LOGGING_REDUCTION = 5;
    //volatile since this can be changed
    private static volatile int MAX_MESSAGE_COUNT_GLOBAL = 10000;
    //read and written by a single thread
    private static int consecutiveOverflowCount = 0;
    private static final long INTERVAL_MINUTES = 5;
    public static final long FIVE_MINUTES_MS = INTERVAL_MINUTES * 60 * 1000;

    private final static long SCHEDULER_INTERVAL = FIVE_MINUTES_MS;

    static
    {
        new Timer(true).scheduleAtFixedRate(new TimerTask()
        {
            @Override
            public void run()
            {
                checkLimitsAndAdjust();
            }
        }, SCHEDULER_INTERVAL, SCHEDULER_INTERVAL);
    }

    /**
     * Set global message limit. The limit is compared against all the logs emitted by all the loggers and if the limit is exceeded, top loggers
     * will have their limit reduced to decrease the log count overall;
     *
     * @param globalLimit
     */
    public static void setGlobalMessageLimit(int globalLimit)
    {
        Preconditions.checkArgument(globalLimit > 0, "global limit must be > 0");
        MAX_MESSAGE_COUNT_GLOBAL = globalLimit;
    }

    /**
     * Get logger by name with the default limits of critical and non-critical message counts per 5 minutes window
     *
     * @param name
     * @return
     */
    public static Logger getLogger(String name)
    {
        return getLogger(name, DEFAULT_MESSAGE_COUNT_PER_LOGGER, DEFAULT_MESSAGE_COUNT_PER_LOGGER);
    }

    /**
     * Get logger with max critical (errors) and non-critical (anything other than errors) set.The message count limits are enforced every 5 minutes.
     *
     * @param name
     * @param nonCriticalMessageLimit
     * @param criticalMessageLimit
     * @return
     */
    public static Logger getLogger(String name, long nonCriticalMessageLimit, long criticalMessageLimit)
    {
        Preconditions.checkArgument(nonCriticalMessageLimit > 0, "nonCriticalMessageLimit must be > 0");
        Preconditions.checkArgument(criticalMessageLimit > 0, "criticalMessageLimit > 0");
        Logger logger = LoggerFactory.getLogger(name);
        return LOGGER_MAP.computeIfAbsent(logger, logger1 -> new ThrottlingLogger(logger1, nonCriticalMessageLimit, criticalMessageLimit));
    }

    /**
     * Get logger by class with the default limits of critical and non-critical message counts per 5 minutes window
     *
     * @param clazz
     * @return
     */
    public static Logger getLogger(Class<?> clazz)
    {
        return getLogger(clazz, DEFAULT_MESSAGE_COUNT_PER_LOGGER, DEFAULT_MESSAGE_COUNT_PER_LOGGER);
    }

    /**
     * Get logger with max critical (errors) and non-critical (anything other than errors) set.The message count limits are enforced every 5 minutes.
     *
     * @param clazz
     * @param nonCriticalMessageLimit
     * @param criticalMessageLimit
     * @return
     */
    public static Logger getLogger(Class<?> clazz, long nonCriticalMessageLimit, long criticalMessageLimit)
    {
        Preconditions.checkArgument(nonCriticalMessageLimit > 0, "nonCriticalMessageLimit must be > 0");
        Preconditions.checkArgument(criticalMessageLimit > 0, "criticalMessageLimit > 0");
        Logger logger = LoggerFactory.getLogger(clazz);
        return LOGGER_MAP.computeIfAbsent(logger, logger1 -> new ThrottlingLogger(logger1, nonCriticalMessageLimit, criticalMessageLimit));
    }

    @VisibleForTesting
    static void checkLimitsAndAdjust()
    {
        Set<MessageCountingLogger> loggersSortedByNonCriticalMessageCount = new TreeSet<MessageCountingLogger>(
                Comparator.comparingLong(MessageCountingLogger::getMessageCount).reversed());
        Reference<Long> totalMessages = new Reference<Long>(0L);
        LOGGER_MAP.values().forEach(throttlingLogger -> {
            long nonCriticalPrevValue = throttlingLogger.nonCriticalMessageCounter.getAndSet(0);
            long supressedNonCriticalMessages = nonCriticalPrevValue - throttlingLogger.nonCriticalMessageLimit;

            long criticalPrevValue = throttlingLogger.criticalMessageCounter.getAndSet(0);
            long supressedCriticalMessages = criticalPrevValue - throttlingLogger.criticalMessageLimit;

            totalMessages.setT(totalMessages.getT() + (nonCriticalPrevValue + criticalPrevValue));
            if (supressedNonCriticalMessages > 0 || supressedCriticalMessages > 0)
            {
                throttlingLogger.underlyingLogger.info("supressed messages in last window ({} minutes): critical {}, non-critical {}", INTERVAL_MINUTES, supressedCriticalMessages,
                        supressedNonCriticalMessages);
            }

            if (nonCriticalPrevValue > 0)
            {
                loggersSortedByNonCriticalMessageCount.add(new MessageCountingLogger(throttlingLogger, nonCriticalPrevValue));
            }
        });

        adjustMessageCountIfNecessary(loggersSortedByNonCriticalMessageCount, totalMessages.getT());
    }

    /**
     * Calculate the message counts against the set global limit. If there is overflow of more than 20%, start reducing the limits for the noisy loggers by
     * 5% until we reduce the overflow. The liits are not changed for the critical messages.
     *
     * @param loggersSortedByNonCriticalMessageCount
     * @param totalMessageCount
     */
    private static void adjustMessageCountIfNecessary(Set<MessageCountingLogger> loggersSortedByNonCriticalMessageCount, long totalMessageCount)
    {
        /**
         * Only bother if we are over 20%
         */
        long sizeOverflow = totalMessageCount - MAX_MESSAGE_COUNT_GLOBAL;
        if (sizeOverflow > MAX_MESSAGE_COUNT_GLOBAL * 0.2)
        {
            consecutiveOverflowCount++;
        } else
        {
            consecutiveOverflowCount = 0;
        }

        if (consecutiveOverflowCount >= MAX_CONSECUTIVE_OVERFLOW_TO_TRIGGER_LOGGING_REDUCTION)
        {
            log.info("message overflow exceeded, going to reduce limit for noisy loggers");
            //take the top 5 offenders and reduce by 5 %
            int index = 0;
            for (MessageCountingLogger logger : loggersSortedByNonCriticalMessageCount)
            {
                ++index;
                long sizeReduction = logger.logger.nonCriticalMessageLimit * PERCENTAGE_TO_REDUCE / 100;
                logger.logger.updateNonCriticalMessageLimit(logger.logger.nonCriticalMessageLimit - sizeReduction);
                sizeOverflow -= sizeReduction;
                log.info("reducing non-critical message limit by {} for logger {}", sizeReduction, logger.logger.getName());
                if (sizeOverflow <= 0 || index >= MAX_OFFENDERS_TO_REDUCE)
                {
                    break;
                }
            }
        }
    }

    /**
     * Logger with count
     */
    private static class MessageCountingLogger
    {
        private final ThrottlingLogger logger;
        private final long messageCount;

        private MessageCountingLogger(ThrottlingLogger logger, long messageCount)
        {
            this.logger = logger;
            this.messageCount = messageCount;
        }

        public long getMessageCount()
        {
            return messageCount;
        }
    }

    /**
     * Throttled logger
     */
    @VisibleForTesting
    static class ThrottlingLogger
            implements Logger
    {
        private final Logger underlyingLogger;
        private final AtomicLong nonCriticalMessageCounter;
        private final long criticalMessageLimit;
        private final AtomicLong criticalMessageCounter;

        private long nonCriticalMessageLimit;

        private ThrottlingLogger(Logger underlyingLogger, long nonCriticalMessageLimit, long criticalMessageLimit)
        {
            this.underlyingLogger = underlyingLogger;
            this.nonCriticalMessageLimit = nonCriticalMessageLimit;
            this.criticalMessageLimit = criticalMessageLimit;
            nonCriticalMessageCounter = new AtomicLong();
            criticalMessageCounter = new AtomicLong();
        }

        private void updateNonCriticalMessageLimit(long updatedNonCriticalMessageLimit)
        {
            nonCriticalMessageLimit = updatedNonCriticalMessageLimit;
        }

        @VisibleForTesting
        long getCriticalMessageCounter()
        {
            return criticalMessageCounter.get();
        }

        @VisibleForTesting
        long getNonCriticalMessageCounter()
        {
            return nonCriticalMessageCounter.get();
        }

        @VisibleForTesting
        long getNonCriticalMessageLimit()
        {
            return nonCriticalMessageLimit;
        }

        @Override
        public String getName()
        {
            return underlyingLogger.getName();
        }

        @Override
        public boolean isTraceEnabled()
        {
            return underlyingLogger.isTraceEnabled();
        }

        @Override
        public void trace(String msg)
        {
            if (isTraceEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(msg);
            }
        }

        @Override
        public void trace(String format, Object arg)
        {
            if (isTraceEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(format, arg);
            }
        }

        @Override
        public void trace(String format, Object arg1, Object arg2)
        {
            if (isTraceEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(format, arg1, arg2);
            }
        }

        @Override
        public void trace(String format, Object... arguments)
        {
            if (isTraceEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(format, arguments);
            }
        }

        @Override
        public void trace(String msg, Throwable t)
        {
            if (isTraceEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(msg, t);
            }
        }

        @Override
        public boolean isTraceEnabled(Marker marker)
        {
            return underlyingLogger.isTraceEnabled(marker);
        }

        @Override
        public void trace(Marker marker, String msg)
        {
            if (isTraceEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(marker, msg);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg)
        {
            if (isTraceEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(marker, format, arg);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2)
        {
            if (isTraceEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(marker, format, arg1, arg2);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray)
        {
            if (isTraceEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(marker, format, argArray);
            }
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t)
        {
            if (isTraceEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.trace(marker, msg, t);
            }
        }

        @Override
        public boolean isDebugEnabled()
        {
            return underlyingLogger.isDebugEnabled();
        }

        @Override
        public void debug(String msg)
        {
            if (isDebugEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(msg);
            }
        }

        @Override
        public void debug(String format, Object arg)
        {
            if (isDebugEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(format, arg);
            }
        }

        @Override
        public void debug(String format, Object arg1, Object arg2)
        {
            if (isDebugEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(format, arg1, arg2);
            }
        }

        @Override
        public void debug(String format, Object... arguments)
        {
            if (isDebugEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(format, arguments);
            }
        }

        @Override
        public void debug(String msg, Throwable t)
        {
            if (isDebugEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(msg, t);
            }
        }

        @Override
        public boolean isDebugEnabled(Marker marker)
        {
            return underlyingLogger.isDebugEnabled(marker);
        }

        @Override
        public void debug(Marker marker, String msg)
        {
            if (isDebugEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(marker, msg);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg)
        {
            if (isDebugEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(marker, format, arg);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2)
        {
            if (isDebugEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(marker, format, arg1, arg2);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments)
        {
            if (isDebugEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(marker, format, arguments);
            }
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t)
        {
            if (isDebugEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.debug(marker, msg, t);
            }
        }

        @Override
        public boolean isInfoEnabled()
        {
            return underlyingLogger.isInfoEnabled();
        }

        @Override
        public void info(String msg)
        {
            if (isInfoEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(msg);
            }
        }

        @Override
        public void info(String format, Object arg)
        {
            if (isInfoEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(format, arg);
            }
        }

        @Override
        public void info(String format, Object arg1, Object arg2)
        {
            if (isInfoEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(format, arg1, arg2);
            }
        }

        @Override
        public void info(String format, Object... arguments)
        {
            if (isInfoEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(format, arguments);
            }
        }

        @Override
        public void info(String msg, Throwable t)
        {
            if (isInfoEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(msg, t);
            }
        }

        @Override
        public boolean isInfoEnabled(Marker marker)
        {
            return underlyingLogger.isInfoEnabled(marker);
        }

        @Override
        public void info(Marker marker, String msg)
        {
            if (isInfoEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(marker, msg);
            }
        }

        @Override
        public void info(Marker marker, String format, Object arg)
        {
            if (isInfoEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(marker, format, arg);
            }
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2)
        {
            if (isInfoEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(marker, format, arg1, arg2);
            }
        }

        @Override
        public void info(Marker marker, String format, Object... arguments)
        {
            if (isInfoEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(marker, format, arguments);
            }
        }

        @Override
        public void info(Marker marker, String msg, Throwable t)
        {
            if (isInfoEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.info(marker, msg, t);
            }
        }

        @Override
        public boolean isWarnEnabled()
        {
            return underlyingLogger.isWarnEnabled();
        }

        @Override
        public void warn(String msg)
        {
            if (isWarnEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(msg);
            }
        }

        @Override
        public void warn(String format, Object arg)
        {
            if (isWarnEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(format, arg);
            }
        }

        @Override
        public void warn(String format, Object... arguments)
        {
            if (isWarnEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(format, arguments);
            }
        }

        @Override
        public void warn(String format, Object arg1, Object arg2)
        {
            if (isWarnEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(format, arg1, arg2);
            }
        }

        @Override
        public void warn(String msg, Throwable t)
        {
            if (isWarnEnabled() && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(msg, t);
            }
        }

        @Override
        public boolean isWarnEnabled(Marker marker)
        {
            return underlyingLogger.isWarnEnabled(marker);
        }

        @Override
        public void warn(Marker marker, String msg)
        {
            if (isWarnEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(marker, msg);
            }
        }

        @Override
        public void warn(Marker marker, String format, Object arg)
        {
            if (isWarnEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(marker, format, arg);
            }
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2)
        {
            if (isWarnEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(marker, format, arg1, arg2);
            }
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments)
        {
            if (isWarnEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(marker, format, arguments);
            }
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t)
        {
            if (isWarnEnabled(marker) && nonCriticalMessageCounter.incrementAndGet() <= nonCriticalMessageLimit)
            {
                underlyingLogger.warn(marker, msg, t);
            }
        }

        @Override
        public boolean isErrorEnabled()
        {
            return underlyingLogger.isErrorEnabled();
        }

        @Override
        public void error(String msg)
        {
            if (isErrorEnabled() && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(msg);
            }
        }

        @Override
        public void error(String format, Object arg)
        {
            if (isErrorEnabled() && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(format, arg);
            }
        }

        @Override
        public void error(String format, Object arg1, Object arg2)
        {
            if (isErrorEnabled() && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(format, arg1, arg2);
            }
        }

        @Override
        public void error(String format, Object... arguments)
        {
            if (isErrorEnabled() && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(format, arguments);
            }
        }

        @Override
        public void error(String msg, Throwable t)
        {
            if (isErrorEnabled() && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(msg, t);
            }
        }

        @Override
        public boolean isErrorEnabled(Marker marker)
        {
            return underlyingLogger.isErrorEnabled(marker);
        }

        @Override
        public void error(Marker marker, String msg)
        {
            if (isErrorEnabled(marker) && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(marker, msg);
            }
        }

        @Override
        public void error(Marker marker, String format, Object arg)
        {
            if (isErrorEnabled(marker) && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(marker, format, arg);
            }
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2)
        {
            if (isErrorEnabled(marker) && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(marker, format, arg1, arg2);
            }
        }

        @Override
        public void error(Marker marker, String format, Object... arguments)
        {
            if (isErrorEnabled(marker) && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(marker, format, arguments);
            }
        }

        @Override
        public void error(Marker marker, String msg, Throwable t)
        {
            if (isErrorEnabled(marker) && criticalMessageCounter.incrementAndGet() <= criticalMessageLimit)
            {
                underlyingLogger.error(marker, msg, t);
            }
        }
    }
}

