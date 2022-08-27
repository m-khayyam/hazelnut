package com.hazelnut.node;

import com.hazelnut.cluster.DistributedLock;
import com.hazelnut.cluster.ZooKeeperSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.hazelnut.HazelNutApplication.NODE_ID;

@Service

/**
 *Service to perform startup activities
 *Includes the responsibility to init cluster if required
 */
public class NodeStartup {

    @Value("${cluster.max.liveness.threshold.time.ms}")
    private Long clusterivenessThreshold;

    @Value("${cluster.status.data.path}")
    private String clusterDataPath;

    @Value("${node.data.path.prefix}")
    private String nodeDataPathPrefix;

    private final ZooKeeperSession clusterData;

    private final DistributedLock distributedLock;

    private final Logger logger = LoggerFactory.getLogger(NodeStartup.class);

    public NodeStartup(@Autowired ZooKeeperSession zooKeeperSession, @Autowired DistributedLock distributedLock) {
        this.clusterData = zooKeeperSession;
        this.distributedLock = distributedLock;
    }

    /**
     * This method checks at cluster level and serve startup message if yet pending for cluster
     * <br/>
     * <br/>
     * It do the welcome job if:<br/>
     * (1) Cluster is never marked as started<br/>
     * (2) Cluster is marked as started already but now no other node is connected. Thus getting this node up is kind of restart<br/>
     * (3) the node is unable to connect to cluster due to network issue
     * <br/>
     * <br/>
     * Please note that in case of network failure or lock failure, we want to proceed with startup message
     * Thus ideally taking lock and checking status first, but in case of problems proceeding to print
     */
    public void bootStrapNodeAndCluster() {

        try (DistributedLock lock = distributedLock.tryLock();
             ZooKeeperSession session = clusterData.open()) {

            boolean clusterInitialized = session.getClusterInitStatus(clusterDataPath, false);

            if (!clusterInitialized || !activeNodesInCluster(session)) {
                logger.info("We are started!");
            }
            if (!clusterInitialized) {
                session.setClusterInitStatus(clusterDataPath, true);
            }
        }
    }


    /**
     * Check if there are other nodes connected to cluster
     *
     * @return true if any other node is active in cluster
     */
    private boolean activeNodesInCluster(ZooKeeperSession session) {
        try {
            List<String> nodeIds = session.getClusterNodes(clusterDataPath);

            Stream<String> siblingNodes = nodeIds.stream()
                    .filter(id -> !id.equals(NODE_ID));

            Optional<Long> latestReportingTime = siblingNodes.map(id -> session.getNodeHeartBeatTime(nodeDataPathPrefix + id, 0L))
                    .max(Comparator.comparing(Long::valueOf));

            if (latestReportingTime.isPresent()) {
                long gap = Instant.now().toEpochMilli() - latestReportingTime.get();
                if (gap < clusterivenessThreshold) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return false;
    }


}
