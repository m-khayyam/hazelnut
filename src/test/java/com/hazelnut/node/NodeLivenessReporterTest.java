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
class NodeLivenessReporterTest extends NodeLivenessReporterTestUtils {


    @Test
    //Node records its presence to cluster by publishing heartbeat time
      void testNodeAcquiresDistributedLockAndPerformsStartup() throws Exception {
        Stream<ILoggingEvent> logsWritten = captureLogsForAppStartUpService(NodeLivenessReporter.class);

        reporter.reportHeartBeatToCluster();

        verifyThatLogsWrite("Published heartbeat to zookeeper", EXACTLY_ONCE, logsWritten);

        verifyHeartBeatCallIsMade(EXACTLY_ONCE);

    }


}
