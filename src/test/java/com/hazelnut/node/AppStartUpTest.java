package com.hazelnut.node;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.hazelnut.node.preps.AppStartUpTestUtils;
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
class AppStartUpTest extends AppStartUpTestUtils {


    @Test
    //Cluster starting first time and this node gets lock to bootstrap
    void testNodeAcquiresDistributedLockAndPerformsStartup() throws Exception {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeStartup.class);

        mockLockIsAcquired();

        mockThatClusterStatusIs(NOT_STARTED);

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", EXACTLY_ONCE, logsWritten);

        verifyThatClusterStartedFlagIsUpdated(EXACTLY_ONCE);
        verifyThatSiblingNodesAreEnquired(NEVER);

    }

    @Test
    //Cluster starting first time and this node fails to lock but proceed with job
    void testNodeFailsToAcquiresDistributedLockAndPerformsStartup() throws Exception {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeStartup.class);

        mockLockAcquiringFails();

        mockThatClusterStatusIs(NOT_STARTED);

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", EXACTLY_ONCE, logsWritten);

        verifyThatClusterStartedFlagIsUpdated(EXACTLY_ONCE);
        verifyThatSiblingNodesAreEnquired(NEVER);

    }

    @Test
    //Cluster is once started and sibling nodes are healthy
    void testNodeAcquiresDistributedLockAndClusterIsAlreadyStarted() throws Exception {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeStartup.class);

        mockLockIsAcquired();

        mockThatClusterStatusIs(STARTED);
        mockSibllingNodesAreConnectedAndReporting();

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", NEVER, logsWritten);
        verifyThatLockWasNotRequired();

        verifyThatClusterStartedFlagIsUpdated(NEVER);
        verifyThatSiblingNodesAreEnquired(EXACTLY_ONCE);
    }

    @Test
    // Cluster already started once, but sibling node not responding and this nodes gets up
    void testNodeAcquiresDistributedLockAndClusterIsRestarted() throws Exception {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeStartup.class);

        mockLockIsAcquired();

        mockThatClusterStatusIs(STARTED);
        mockSibllingNodesAreNotConnected();

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", EXACTLY_ONCE, logsWritten);
        verifyThatLockWasNotRequired();
        verifyThatClusterStartedFlagIsUpdated(NEVER);
        verifyThatSiblingNodesAreEnquired(EXACTLY_ONCE);
    }



    @Configuration
    @Import(NodeStartup.class)
    static class Config {


    }
}
