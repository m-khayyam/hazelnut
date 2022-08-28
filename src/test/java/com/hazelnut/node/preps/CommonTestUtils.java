package com.hazelnut.node.preps;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommonTestUtils {


    protected static final int EXACTLY_ONCE = 1;
    protected static final int TWICE = 2;
    protected static final int NEVER = 0;
    protected static final boolean STARTED = true;
    protected static final boolean NOT_STARTED = false;
    protected static final boolean AND_AGAIN_NOT_STARTED = false;
    protected static final boolean BUT_THEN_STARTED = true;

    protected Stream<ILoggingEvent> captureLogsForAppStartUpService(Class clazz) {
        Logger logger = (Logger) LoggerFactory.getLogger(clazz);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
        Stream<ILoggingEvent> logsWritten = listAppender.list.stream();
        return logsWritten;
    }

    protected void verifyThatLogsWrite(String message, int expected, Stream<ILoggingEvent> logsWritten) {
        assertEquals(expected, logsWritten.filter(l -> l.getMessage().equals(message)).count());
    }
}
