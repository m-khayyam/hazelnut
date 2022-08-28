package com.hazelnut.node;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.hazelnut.node.preps.NodeLivenessReporterTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.stream.Stream;

@SpringBootTest
@RunWith(SpringRunner.class)
@ExtendWith(SpringExtension.class)
@SpringJUnitConfig(NodeLivenessReporter.class)
class TestNodeLivenessReporter extends NodeLivenessReporterTestUtils {


    @Test
        //Node don't report its presence to cluster its own startup is pending
    void testTheNodeDontReportLivenessIfNotStartedItself() {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeLivenessReporter.class);

        mockThatNodeIs(NOT_STARTED);

        reporter.updateClusterStatus();

        verifyThatLogsWrite("Updated cluster status as active.", NEVER, logsWritten);

        verifyHeartBeatCallIsMade(NEVER);

    }

    @Test
        //Node reports its presence to cluster and updates cluster status
    void testTheNodeReportsLivenessIfStartedItself() {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeLivenessReporter.class);

        mockZooKeeperSession();

        mockThatNodeIs(STARTED);

        reporter.updateClusterStatus();

        verifyThatLogsWrite("Updated cluster status as active.", EXACTLY_ONCE, logsWritten);

        verifyHeartBeatCallIsMade(EXACTLY_ONCE);

    }
}
