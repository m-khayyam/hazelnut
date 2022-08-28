package com.hazelnut.node;

import com.hazelnut.cluster.ZooKeeperSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class NodeLivenessReporter {
    @Value("${cluster.nodes.liveness.ttl.ms}")
    private long ttl;

    @Value("${cluster.status.data.path}")
    private String clusterActivityTimeRefPath;

    private final ZooKeeperSession zooKeeperSession;
    private final NodeStartup node;

    Logger logger = LoggerFactory.getLogger(NodeLivenessReporter.class);

    public NodeLivenessReporter(@Autowired ZooKeeperSession zooKeeperSession, @Autowired NodeStartup node) {
        this.zooKeeperSession = zooKeeperSession;
        this.node = node;
    }

    @Scheduled(fixedDelayString = "${node.liveness.reporting.time.ms}")
    @Async
    /**
     * Report the heart beat to ZooKeeper after every fixed delay
     */
    public void updateClusterStatus() {
        if (node.isNodeStarted()) {
            try (ZooKeeperSession session = zooKeeperSession.open()) {
                session.markClusterAsActive(clusterActivityTimeRefPath, ttl);
                logger.info("Updated cluster status as active.");
            }
        }
    }
}
