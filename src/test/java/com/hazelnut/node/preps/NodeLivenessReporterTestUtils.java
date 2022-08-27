package com.hazelnut.node.preps;

import com.hazelnut.cluster.ZooKeeperSession;
import com.hazelnut.node.NodeLivenessReporter;
import com.hazelnut.node.NodeStartup;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;

public class NodeLivenessReporterTestUtils extends CommonTestUtils {

    @MockBean
    protected ZooKeeperSession zooKeeperSession;

    @MockBean
    protected NodeStartup nodeStartup;

    @Autowired
    protected NodeLivenessReporter reporter;


    protected void mockZooKeeperSession() {
        Mockito.when(zooKeeperSession.open()).thenReturn(zooKeeperSession);
    }

    protected void verifyHeartBeatCallIsMade() {
        Mockito.verify(zooKeeperSession, times(EXACTLY_ONCE)).setNodeHeartBeatTime(anyString(), anyLong(), anyLong());
    }

}
