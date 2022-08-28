package com.hazelnut.node.preps;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.hazelnut.cluster.DistributedLock;
import com.hazelnut.cluster.ZooKeeperSession;
import com.hazelnut.node.NodeLivenessReporter;
import com.hazelnut.node.NodeStartup;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;

public class NodeStartUpTestUtils extends CommonTestUtils {

    @MockBean
    protected DistributedLock distributedLock;

    @MockBean
    protected ZooKeeperSession session;

    @MockBean
    protected NodeLivenessReporter reporter;

    @Autowired
    protected NodeStartup service;

    protected void mockThatClusterStatusIs(boolean value) {
        Mockito.when(session.getClusterStatus(anyString())).thenReturn(value);
    }

    protected void mockThatClusterStatusIs(boolean value, boolean secondValue) {
        Mockito.when(session.getClusterStatus(anyString())).thenReturn(value, secondValue);
    }

    protected void verifyThatLogsWrite(String message, int expected, Stream<ILoggingEvent> logsWritten) {
        assertEquals(expected, logsWritten.filter(l -> l.getMessage().equals(message)).count());
    }

    protected void mockTheDistributedLock() {
        Mockito.when(distributedLock.tryLock()).thenReturn(distributedLock);
    }

    protected void verifyThatLockIsReleased() {
        Mockito.verify(distributedLock, times(EXACTLY_ONCE)).close();
    }

    protected void verifyThatLockWasNotRequiredOrTried() {
        Mockito.verify(distributedLock, times(NEVER)).tryLock();
    }

    protected void verifyThatClusterStartedFlagIsUpdated(int count) {
        Mockito.verify(session, times(count)).markClusterAsActive(anyString(), anyLong());
    }

    protected void verifyThatClusterStatusIsChecked(int count) {
        Mockito.verify(session, times(count)).getClusterStatus(anyString());
    }

    protected void mockZooKeeperSession() {
        Mockito.when(session.open()).thenReturn(session);
    }

}
