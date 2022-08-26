package com.hazelnut.cluster;

import com.hazelnut.utils.ZkConnectionManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

import static com.hazelnut.utils.DataMapper.*;

@Service
/**
 * Data access layer to fetch or set data from/to ZooKeeper cluster meta
 */
public class ZkDataStore {
    private ZkConnectionManager zkConnectionManager;
    private Logger logger = LoggerFactory.getLogger(ZkDataStore.class);


    public ZkDataStore(@Autowired ZkConnectionManager zkConnectionManager) {
        this.zkConnectionManager = zkConnectionManager;

    }

    /**
     * Checks the startup status of the cluster from Apache ZooKeeper
     *
     * @param clusterStatusPath
     * @param fallBackValue
     * @return True if cluster is already marked as started
     */
    public boolean getClusterInitStatus(String clusterStatusPath, boolean fallBackValue) {
        byte[] data = null;
        try {
            data = zkConnectionManager.activeConnection().getData().forPath(clusterStatusPath);
            return data != null && bytesToBoolean(data);
        } catch (KeeperException.NoNodeException e) {
            logger.info("Cluster status never reported. Thus unable to fetch status.");
            logger.warn(e.getMessage());
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return fallBackValue;
    }

    /**
     * Sets the startup status of the cluster in Apache ZooKeeper
     *
     * @param clusterStatusPath
     * @param status
     */
    public void setClusterInitStatus(String clusterStatusPath, boolean status) {
        byte[] data = booleanToBytes(status);
        try {
            zkConnectionManager.activeConnection().create().orSetData().creatingParentContainersIfNeeded().forPath(clusterStatusPath, data);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    /**
     * List the nodes which are reporting to the cluster
     *
     * @param clusterPath
     * @return list of nodes ids
     * @throws Exception
     */
    public List<String> getClusterNodes(String clusterPath) throws Exception {
        List<String> nodes = null;
        try {
            nodes = zkConnectionManager.activeConnection().getChildren().forPath(clusterPath);
        } catch (KeeperException.NoNodeException | KeeperException.SessionExpiredException ex) {
            logger.warn(ex.getMessage(), ex);
        } catch (IllegalStateException e) {
            zkConnectionManager.resetConnection();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return nodes != null ? nodes : Collections.emptyList();
    }

    /**
     * get the time in epoch millis for a node when it last communicated its health to ZooKeeper
     *
     * @param nodePath
     * @param fallBackValue
     * @return node's latest health reporting time
     */
    public Long getNodeHeartBeatTime(String nodePath, Long fallBackValue) {
        byte[] data = null;
        try {
            data = zkConnectionManager.activeConnection().getData().forPath(nodePath);
        } catch (KeeperException.NoNodeException e) {
            logger.warn("Heart beats are either not recorded by other nodes or got removed as per ttl.");
            logger.warn(e.getMessage());
        } catch (IllegalStateException e) {
            zkConnectionManager.resetConnection();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return data != null ? bytesToLong(data) : fallBackValue;
    }

    /**
     * report the heart beat of this running node to ZooKeeper
     *
     * @param nodePath
     * @param timeMillis
     * @param ttl        i.e. time to live for this data, as we don't need older records to establish cluster's health later
     * @throws IllegalStateException
     */
    public void setNodeHeartBeatTime(String nodePath, long timeMillis, long ttl) throws IllegalStateException {
        try {
            if (ttl > 0) {
                zkConnectionManager.activeConnection().create().orSetData().withTtl(ttl).creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_WITH_TTL).forPath(nodePath, longToBytes(timeMillis));
            } else {
                zkConnectionManager.activeConnection().create().orSetData().creatingParentContainersIfNeeded().forPath(nodePath, longToBytes(timeMillis));
            }
        } catch (IllegalStateException e) {
            zkConnectionManager.resetConnection();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

}
