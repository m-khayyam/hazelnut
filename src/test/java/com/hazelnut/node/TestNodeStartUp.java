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

        mockZooKeeperSession();
        mockTheDistributedLock();
        mockThatClusterStatusIs(NOT_STARTED, AND_AGAIN_NOT_STARTED);

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", EXACTLY_ONCE, logsWritten);

        verifyThatClusterStartedFlagIsUpdated(EXACTLY_ONCE);
        verifyThatClusterStatusIsChecked(TWICE);
        verifyThatLockIsReleased();
    }

    @Test
    //Cluster starting first time and this node gets lock to bootstrap
    void testNodeAcquiresDistributedLockAndPerformsStartup1() {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeStartup.class);

        mockZooKeeperSession();
        mockTheDistributedLock();
        mockThatClusterStatusIs(NOT_STARTED, BUT_THEN_STARTED);

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", NEVER, logsWritten);

        verifyThatClusterStartedFlagIsUpdated(NEVER);
        verifyThatClusterStatusIsChecked(TWICE);
        verifyThatLockIsReleased();
    }


    @Test
    //Cluster is once started and sibling nodes are healthy
    void testNodeAcquiresDistributedLockAndClusterIsAlreadyStarted() {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeStartup.class);

        mockZooKeeperSession();
        mockTheDistributedLock();
        mockThatClusterStatusIs(STARTED);

        service.bootStrapNodeAndCluster();

        verifyThatLogsWrite("We are started!", NEVER, logsWritten);
        verifyThatClusterStatusIsChecked(EXACTLY_ONCE);
        verifyThatClusterStartedFlagIsUpdated(NEVER);
        verifyThatLockWasNotRequiredOrTried();
    }


    @Configuration
    @Import(NodeStartup.class)
    static class Config {
    }
}
