package com.hazelnut.node;

import com.hazelnut.cluster.ZkDataStore;
import com.hazelnut.utils.ZkConnectionManager;
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
public class NodeStartup {
    @Value("${distributed.lock.path}")
    private String lockPath;

    @Value("${cluster.max.liveness.threshold.time.ms}")
    private Long clusterivenessThreshold;

    @Value("${cluster.status.data.path}")
    private String clusterDataPath;

    @Value("${node.data.path.prefix}")
    private String nodeDataPathPrefix;

    @Value("${distributed.lock.timeout.ms}")
    private long timeoutMillis;

    private ZkDataStore clusterData;

    private ZkConnectionManager zkConnectionManager;


    private Logger logger = LoggerFactory.getLogger(NodeStartup.class);

    public NodeStartup(@Autowired ZkDataStore zkDataStore
            , @Autowired ZkConnectionManager zkConnectionManager) {
        this.clusterData = zkDataStore;
        this.zkConnectionManager = zkConnectionManager;
    }

    public void bootStrapNodeAndCluster() {

        boolean clusterInitialized = clusterData.getClusterInitStatus(clusterDataPath, false);
        boolean lockRequired = false;
        try {
            if (!clusterInitialized) {
                lockRequired = true;
                zkConnectionManager.acquireDistributedLock(timeoutMillis, lockPath);
                clusterInitialized = clusterData.getClusterInitStatus(clusterDataPath, false);
            }
            // Pleae note that in case of network failure or lock failure, we want to proceed with action
            // thus ideally taking lock and checking status first, but in case of problems proceeding to print
            if (!clusterInitialized || !activeNodesInCluster()) {
                logger.info("We are started!");
            }
            if (!clusterInitialized ) {
                clusterData.setClusterInitStatus(clusterDataPath, true);
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        } finally {
            if (lockRequired) {
                zkConnectionManager.releaseLock(lockPath);
            }
        }
    }


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
