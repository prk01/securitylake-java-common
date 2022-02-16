package org.securitylake.common.util;

import org.mockito.Mockito;
import org.slf4j.Marker;
import org.testng.Assert;
import org.testng.annotations.Test;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

public class ThrottlingLoggerFactoryTest
{
    @Test
    public void testLogging()
    {
        ThrottlingLoggerFactory.checkLimitsAndAdjust();
        TestLogger logger = TestLoggerFactory.getTestLogger(ThrottlingLoggerFactoryTest.class);
        ThrottlingLoggerFactory.ThrottlingLogger log = (ThrottlingLoggerFactory.ThrottlingLogger) ThrottlingLoggerFactory.getLogger(ThrottlingLoggerFactoryTest.class, 20, 1);
        ThrottlingLoggerFactory.setGlobalMessageLimit(50);
        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 0);
        log.trace("trace message");
        log.debug("debug message");
        log.info("info message");
        log.warn("warn message");
        log.debug("debug message");
        for (int i = 0; i < 20; i++)
        {
            log.info("info message");
        }

        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 25);

        Assert.assertEquals(20, logger.getAllLoggingEvents().size());

        log.error("error message");
        log.error("error message");

        Assert.assertEquals(log.getCriticalMessageCounter(), 2);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 25);
        Assert.assertEquals(21, logger.getAllLoggingEvents().size());

        //adjust limits
        ThrottlingLoggerFactory.checkLimitsAndAdjust();
        logger.clearAll();
        //test again
        log.trace("trace message {} ", "arg1");
        log.debug("debug message {} ", "arg1");
        log.error("error message {} ", "arg1");

        Assert.assertEquals(log.getCriticalMessageCounter(), 1);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 2);
        Assert.assertEquals(3, logger.getAllLoggingEvents().size());

        Assert.assertEquals(log.getNonCriticalMessageLimit(), 20);
        /**
         * log multiple messages to take us over the limit
         */

        for (int times = 0; times <= 6; times++)
        {
            for (int i = 0; i < 25; i++)
            {
                log.debug("message");
                log.debug("message", new RuntimeException());
                log.debug(Mockito.mock(Marker.class), "message");
                log.debug("message with arg {}", "str");

            }
            //adjust limits
            ThrottlingLoggerFactory.checkLimitsAndAdjust();
        }

        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 0);

        Assert.assertEquals(log.getNonCriticalMessageLimit(), 19);
    }

    @Test
    public void testLoggingWithNamedLogger()
    {
        ThrottlingLoggerFactory.checkLimitsAndAdjust();
        TestLogger logger = TestLoggerFactory.getTestLogger("foo");
        ThrottlingLoggerFactory.ThrottlingLogger log = (ThrottlingLoggerFactory.ThrottlingLogger) ThrottlingLoggerFactory.getLogger("foo", 20, 1);
        ThrottlingLoggerFactory.setGlobalMessageLimit(50);
        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 0);
        log.trace("trace message");
        log.debug("debug message");
        log.info("info message");
        log.warn("warn message");
        log.debug("debug message");
        for (int i = 0; i < 20; i++)
        {
            log.info("info message");
        }

        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 25);

        Assert.assertEquals(20, logger.getAllLoggingEvents().size());

    }

    @Test
    public void testLoggingWithDefaultNamedLogger()
    {
        ThrottlingLoggerFactory.checkLimitsAndAdjust();
        TestLogger logger = TestLoggerFactory.getTestLogger("testLoggingWithDefaultLogger");
        ThrottlingLoggerFactory.ThrottlingLogger log = (ThrottlingLoggerFactory.ThrottlingLogger) ThrottlingLoggerFactory.getLogger("testLoggingWithDefaultLogger");
        ThrottlingLoggerFactory.setGlobalMessageLimit(50);
        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 0);
        log.trace("trace message");
        log.debug("debug message");
        log.info("info message");
        log.warn("warn message");
        log.debug("debug message");

        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 5);

        Assert.assertEquals(5, logger.getAllLoggingEvents().size());

    }

    @Test
    public void testLoggingWithDefaultCalssLogger()
    {
        ThrottlingLoggerFactory.checkLimitsAndAdjust();
        TestLogger logger = TestLoggerFactory.getTestLogger(String.class);
        ThrottlingLoggerFactory.ThrottlingLogger log = (ThrottlingLoggerFactory.ThrottlingLogger) ThrottlingLoggerFactory.getLogger(String.class);
        ThrottlingLoggerFactory.setGlobalMessageLimit(50);
        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 0);
        log.trace("trace message");
        log.debug("debug message");
        log.info("info message");
        log.warn("warn message");
        log.debug("debug message");

        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 5);

        Assert.assertEquals(5, logger.getAllLoggingEvents().size());

    }

    @Test
    public void testLoggingWithAllTypesOfLogMethods()
    {
        ThrottlingLoggerFactory.checkLimitsAndAdjust();
        TestLogger logger = TestLoggerFactory.getTestLogger("testLoggingWithAllTypesOfLogMethods");
        ThrottlingLoggerFactory.ThrottlingLogger log = (ThrottlingLoggerFactory.ThrottlingLogger) ThrottlingLoggerFactory.getLogger("testLoggingWithAllTypesOfLogMethods");
        ThrottlingLoggerFactory.setGlobalMessageLimit(50);
        Assert.assertEquals(log.getCriticalMessageCounter(), 0);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 0);
        Marker markerMock = Mockito.mock(Marker.class);
        log.debug("debug no arg");
        log.debug("debug {} arg", "one");
        log.debug("debug {} {} arg", "one", "two");
        log.debug("debug {} {} {} arg", 1, 2, 3);
        log.debug("debug no arg", new RuntimeException());

        //with marker

        log.debug(markerMock, "debug no arg");
        log.debug(markerMock, "debug {} arg", "one");
        log.debug(markerMock, "debug {} {} arg", "one", "two");
        log.debug(markerMock, "debug {} {} {} arg", 1, 2, 3);
        log.debug(markerMock, "debug no arg", new RuntimeException());

        log.info("info no arg");
        log.info("info {} arg", "one");
        log.info("info {} {} arg", "one", "two");
        log.info("info {} {} {} arg", 1, 2, 3);
        log.info("info no arg", new RuntimeException());

        //with marker

        log.info(markerMock, "info no arg");
        log.info(markerMock, "info {} arg", "one");
        log.info(markerMock, "info {} {} arg", "one", "two");
        log.info(markerMock, "info {} {} {} arg", 1, 2, 3);
        log.info(markerMock, "info no arg", new RuntimeException());

        log.trace("trace no arg");
        log.trace("trace {} arg", "one");
        log.trace("trace {} {} arg", "one", "two");
        log.trace("trace {} {} {} arg", 1, 2, 3);
        log.trace("trace no arg", new RuntimeException());

        //with marker

        log.trace(markerMock, "trace no arg");
        log.trace(markerMock, "trace {} arg", "one");
        log.trace(markerMock, "trace {} {} arg", "one", "two");
        log.trace(markerMock, "trace {} {} {} arg", 1, 2, 3);
        log.trace(markerMock, "trace no arg", new RuntimeException());

        log.warn("warn no arg");
        log.warn("warn {} arg", "one");
        log.warn("warn {} {} arg", "one", "two");
        log.warn("warn {} {} {} arg", 1, 2, 3);
        log.warn("warn no arg", new RuntimeException());

        //with marker

        log.warn(markerMock, "trace no arg");
        log.warn(markerMock, "trace {} arg", "one");
        log.warn(markerMock, "trace {} {} arg", "one", "two");
        log.warn(markerMock, "trace {} {} {} arg", 1, 2, 3);
        log.warn(markerMock, "trace no arg", new RuntimeException());

        log.error("error no arg");
        log.error("error {} arg", "one");
        log.error("error {} {} arg", "one", "two");
        log.error("error {} {} {} arg", 1, 2, 3);
        log.error("error no arg", new RuntimeException());

        //with marker

        log.error(markerMock, "error no arg");
        log.error(markerMock, "error {} arg", "one");
        log.error(markerMock, "error {} {} arg", "one", "two");
        log.error(markerMock, "error {} {} {} arg", 1, 2, 3);
        log.error(markerMock, "error no arg", new RuntimeException());

        Assert.assertEquals(log.getCriticalMessageCounter(), 10);
        Assert.assertEquals(log.getNonCriticalMessageCounter(), 40);

        Assert.assertEquals(50, logger.getAllLoggingEvents().size());

    }

    @Test
    public void testLoggingWithIncorrectLimit()
    {
        createLoggerAndExpectException("foo", -1, 10);
        createLoggerAndExpectException("foo", 1, -10);
        createLoggerAndExpectException(ThrottlingLoggerFactoryTest.class, -1, 10);
        createLoggerAndExpectException(ThrottlingLoggerFactoryTest.class, 1, -10);
    }

    private void createLoggerAndExpectException(String name, int nonCriticalLimit, int criticalLimit)
    {
        try
        {
            ThrottlingLoggerFactory.getLogger(name, nonCriticalLimit, criticalLimit);
        } catch (IllegalArgumentException e)
        {
            return;
        }
        throw new RuntimeException("shouldn't be here");
    }

    private void createLoggerAndExpectException(Class<?> clazz, int nonCriticalLimit, int criticalLimit)
    {
        try
        {
            ThrottlingLoggerFactory.getLogger(clazz, nonCriticalLimit, criticalLimit);
        } catch (IllegalArgumentException e)
        {
            return;
        }
        throw new RuntimeException("shouldn't be here");
    }
}
