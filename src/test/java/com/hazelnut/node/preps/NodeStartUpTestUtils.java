package com.hazelnut.node.preps;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.hazelnut.cluster.DistributedLock;
import com.hazelnut.cluster.ZkDataStore;
import com.hazelnut.node.NodeLivenessReporter;
import com.hazelnut.node.NodeStartup;
import com.hazelnut.utils.ZkConnectionManager;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.hazelnut.HazelNutApplication.NODE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;

public class NodeStartUpTestUtils extends CommonTestUtils {

    @MockBean
    protected ZkConnectionManager zkConnectionManager;

    @MockBean
    protected DistributedLock distributedLock;

    @MockBean
    protected ZkDataStore clusterData;

    @MockBean
    protected NodeLivenessReporter reporter;

    @Autowired
    protected NodeStartup service;

    protected void mockThatClusterStatusIs(boolean value) {
        Mockito.when(clusterData.getClusterInitStatus(anyString(), anyBoolean())).thenReturn(value);
    }

    protected void verifyThatLogsWrite(String message, int expected, Stream<ILoggingEvent> logsWritten) {
        assertEquals(expected, logsWritten.filter(l -> l.getMessage().equals(message)).count());
    }

    protected void mockLockIsAcquired() {
        Mockito.when(distributedLock.tryLock()).thenReturn(true);
    }

    protected void verifyThatLockIsReleased() {
        Mockito.verify(distributedLock, times(EXACTLY_ONCE)).releaseLock();
    }

    protected void mockLockAcquiringFails() {
        Mockito.when(distributedLock.tryLock()).thenReturn(false);
    }

    protected void verifyThatLockWasNotRequired() {
        Mockito.verify(distributedLock, times(NEVER)).tryLock();
    }

    protected void verifyThatClusterStartedFlagIsUpdated(int count) {
        Mockito.verify(clusterData, times(count)).setClusterInitStatus(anyString(), anyBoolean());
    }

    protected void mockSibllingNodesAreNotConnected() throws Exception {
        Mockito.when(clusterData.getClusterNodes(anyString())).thenReturn(Arrays.asList(NODE_ID, "siblingNode"));
        Mockito.when(clusterData.getNodeHeartBeatTime(anyString(), anyLong())).thenReturn(0l);
    }

    protected void mockSibllingNodesAreConnectedAndReporting() throws Exception {
        Mockito.when(clusterData.getClusterNodes(anyString())).thenReturn(Arrays.asList(NODE_ID, "siblingNode"));
        Mockito.when(clusterData.getNodeHeartBeatTime(anyString(), anyLong())).thenReturn(Instant.now().toEpochMilli());
    }

    protected void verifyThatSiblingNodesAreEnquired(int count) {
        try {
            Mockito.verify(clusterData, times(count)).getClusterNodes(anyString());
            Mockito.verify(clusterData, times(count)).getNodeHeartBeatTime(anyString(), anyLong());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
