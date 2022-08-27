package com.hazelnut.node;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.hazelnut.node.preps.NodeStartUpTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.stream.Stream;

@SpringBootTest
@RunWith(SpringRunner.class)
@ExtendWith(SpringExtension.class)
class TestNodeStartUp extends NodeStartUpTestUtils {

    @Test
    //Cluster starting first time and this node gets lock to bootstrap
    void testNodeAcquiresDistributedLockAndPerformsStartup() {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeStartup.class);

        mockTheDistributedLock();
        mockThatClusterStatusIs(NOT_STARTED);

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", EXACTLY_ONCE, logsWritten);

        verifyThatClusterStartedFlagIsUpdated(EXACTLY_ONCE);
        verifyThatSiblingNodesAreEnquired(NEVER);
        verifyThatLockIsReleased();
    }


    @Test
    //Cluster is once started and sibling nodes are healthy
    void testNodeAcquiresDistributedLockAndClusterIsAlreadyStarted() {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeStartup.class);

        mockTheDistributedLock();
        mockThatClusterStatusIs(STARTED);
        mockSiblingNodesAreConnectedAndReporting();

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", NEVER, logsWritten);
        verifyThatClusterStartedFlagIsUpdated(NEVER);
        verifyThatSiblingNodesAreEnquired(EXACTLY_ONCE);
        verifyThatLockIsReleased();
    }

    @Test
    // Cluster already started once, but sibling node not responding
    void testNodeAcquiresDistributedLockAndClusterIsRestarted() {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeStartup.class);

        mockTheDistributedLock();
        mockThatClusterStatusIs(STARTED);
        mockSiblingNodesAreNotConnected();

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", EXACTLY_ONCE, logsWritten);
        verifyThatClusterStartedFlagIsUpdated(NEVER);
        verifyThatSiblingNodesAreEnquired(EXACTLY_ONCE);
        verifyThatLockIsReleased();
    }

    @Configuration
    @Import(NodeStartup.class)
    static class Config {
    }
}
