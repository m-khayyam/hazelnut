package com.hazelnut.node;

import com.hazelnut.cluster.DistributedLock;
import com.hazelnut.cluster.ZooKeeperSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service

/**
 *Service to perform startup activities
 *Includes the responsibility to init cluster if required
 */
public class NodeStartup {

    private boolean nodeStarted = false;

    @Value("${cluster.status.data.path}")
    private String clusterActivityTimeRefPath;

    @Value("${cluster.nodes.liveness.ttl.ms}")
    private long ttl;

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

        try (ZooKeeperSession session = clusterData.open()) {
            if (!session.getClusterStatus(clusterActivityTimeRefPath)) {
                try (DistributedLock lock = distributedLock.tryLock()) {
                    if (!session.getClusterStatus(clusterActivityTimeRefPath)) {
                        logger.info("We are started!");
                        session.markClusterAsActive(clusterActivityTimeRefPath, ttl);
                    }
                }
            }
            nodeStarted = true;
        }
    }

    public boolean isNodeStarted() {
        return nodeStarted;
    }
}
