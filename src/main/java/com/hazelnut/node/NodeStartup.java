package com.hazelnut.node;

import com.hazelnut.cluster.DistributedLock;
import com.hazelnut.cluster.ZkDataStore;
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

    private ZkDataStore clusterData;

    private DistributedLock distributedLock;

    private Logger logger = LoggerFactory.getLogger(NodeStartup.class);

    public NodeStartup(@Autowired ZkDataStore zkDataStore, @Autowired DistributedLock distributedLock) {
        this.clusterData = zkDataStore;
        this.distributedLock = distributedLock;
    }

    /**
     * No Starts up and checks if cluster is already started other nodes are connected to it
     * if cluster never started OR other nodes not connected and this is one is only in starting phase then serve starup message
     * <p>
     * Please note that in case of network failure or lock failure, we want to proceed with startup message
     * Thus ideally taking lock and checking status first, but in case of problems proceeding to print
     */
    public void bootStrapNodeAndCluster() {

        boolean clusterInitialized = clusterData.getClusterInitStatus(clusterDataPath, false);
        boolean lockAcquired = false;
        try {
            if (!clusterInitialized) {
                lockAcquired = distributedLock.tryLock();
                clusterInitialized = clusterData.getClusterInitStatus(clusterDataPath, false);
            }

            if (!clusterInitialized || !activeNodesInCluster()) {
                logger.info("We are started!");
            }
            if (!clusterInitialized) {
                clusterData.setClusterInitStatus(clusterDataPath, true);
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        } finally {
            if (lockAcquired) {
                distributedLock.releaseLock();
            }
        }
    }


    /**
     * Check if there are other nodes connected to cluster
     *
     * @return true if any other node is active in cluster
     */
    private boolean activeNodesInCluster() {
        try {
            List<String> nodeIds = clusterData.getClusterNodes(clusterDataPath);

            Stream<String> siblingNodes = nodeIds.stream()
                    .filter(id -> !id.equals(NODE_ID));

            Optional<Long> latestReportingTime = siblingNodes.map(id -> clusterData.getNodeHeartBeatTime(nodeDataPathPrefix + id, 0l))
                    .max(Comparator.comparing(Long::valueOf));

            if (latestReportingTime.isPresent()) {
                Long gap = Instant.now().toEpochMilli() - latestReportingTime.get();
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
