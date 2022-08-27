package com.hazelnut.cluster;

import com.hazelnut.utils.ZkClientSupplier;
import org.apache.curator.framework.CuratorFramework;
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
    private final ZkClientSupplier zkClientSupplier;
    private final Logger logger = LoggerFactory.getLogger(ZkDataStore.class);


    public ZkDataStore(@Autowired ZkClientSupplier zkClientSupplier) {
        this.zkClientSupplier = zkClientSupplier;

    }

    /**
     * Checks the startup status of the cluster from Apache ZooKeeper
     *
     * @param clusterStatusPath
     * @param fallBackValue
     * @return True if cluster is already marked as started
     */
    public boolean getClusterInitStatus(String clusterStatusPath, boolean fallBackValue) {
        byte[] data;
        try (CuratorFramework client = zkClientSupplier.newZkClient()) {
            data = client.getData().forPath(clusterStatusPath);
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
        try (CuratorFramework client = zkClientSupplier.newZkClient()) {
            client.create().orSetData().creatingParentContainersIfNeeded().forPath(clusterStatusPath, data);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    /**
     * List the nodes which are reporting to the cluster
     *
     * @param clusterPath
     * @return list of nodes ids
     */
    public List<String> getClusterNodes(String clusterPath) {
        List<String> nodes = null;
        try (CuratorFramework client = zkClientSupplier.newZkClient()) {
            nodes = client.getChildren().forPath(clusterPath);
        } catch (KeeperException.NoNodeException ex) {
            logger.warn("No connected nodes data retrieved. As no node recently reported within data ttl time.");
            logger.warn(ex.getMessage());
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
    public long getNodeHeartBeatTime(String nodePath, long fallBackValue) {
        byte[] data = null;
        try (CuratorFramework client = zkClientSupplier.newZkClient()) {
            data = client.getData().forPath(nodePath);
        } catch (KeeperException.NoNodeException e) {
            logger.warn("Heart beats are either not recorded by other nodes or got removed as per ttl.");
            logger.warn(e.getMessage());
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
     */
    public void setNodeHeartBeatTime(String nodePath, long timeMillis, long ttl) {
        try (CuratorFramework client = zkClientSupplier.newZkClient()) {
            if (ttl > 0) {
                client.create().orSetData().withTtl(ttl).creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_WITH_TTL).forPath(nodePath, longToBytes(timeMillis));
            } else {
                client.create().orSetData().creatingParentContainersIfNeeded().forPath(nodePath, longToBytes(timeMillis));
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

}
