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
public class ZkDataStore {
    private ZkConnectionManager zkConnectionManager;
    private Logger logger = LoggerFactory.getLogger(ZkDataStore.class);


    public ZkDataStore(@Autowired ZkConnectionManager zkConnectionManager) {
        this.zkConnectionManager = zkConnectionManager;

    }

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

    public void setClusterInitStatus(String clusterStatusPath, boolean status) {
        byte[] data = booleanToBytes(status);
        try {
            zkConnectionManager.activeConnection().create().orSetData().creatingParentContainersIfNeeded().forPath(clusterStatusPath, data);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }


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
