package com.hazelnut.node;

import com.hazelnut.cluster.ZooKeeperSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;

import static com.hazelnut.HazelNutApplication.NODE_ID;

@Service
public class NodeLivenessReporter {
    private final ZooKeeperSession zooKeeperSession;

    public NodeLivenessReporter(@Autowired ZooKeeperSession zooKeeperSession) {
        this.zooKeeperSession = zooKeeperSession;
    }

    @Value("${cluster.nodes.liveness.ttl.ms}")
    private long ttl;

    @Value("${node.data.path.prefix}")
    private String dataPath;


    @Value("${node.data.path.prefix}")
    private String nodeDataPathPrefix;

    Logger logger = LoggerFactory.getLogger(NodeLivenessReporter.class);

    @Scheduled(fixedDelayString = "${node.liveness.reporting.time.ms}")
    @Async
    /**
     * Report the heart beat to ZooKeeper after every fixed delay
     */
    public void reportHeartBeatToCluster() {
        try (ZooKeeperSession session = zooKeeperSession.open()) {
            session.setNodeHeartBeatTime(nodeDataPathPrefix + NODE_ID, Instant.now().toEpochMilli(), ttl);
            logger.info("Published heartbeat to zookeeper");
        }
    }
}
