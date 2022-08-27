package com.hazelnut.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.hazelnut.utils.DataMapper.*;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;

@Service
/**
 * Data access layer to fetch or set data from/to ZooKeeper cluster meta
 */
@Scope("prototype")
public class ZooKeeperSession implements Closeable {
    @Value("${client.session.timeout.ms}")
    private int sessionTimeoutMs;

    @Value("${client.connection.timeout.ms}")
    private int connectionTimeoutMs;

    @Value("${client.connection.string}")
    private String connectionString;

    @Value("${client.retry.time.ms}")
    private int retryTime;

    @Value("${client.retry.attempts.count}")
    private int numberOfTries;

    private final Logger logger = LoggerFactory.getLogger(ZooKeeperSession.class);

    private CuratorFramework client;


    /**
     * Open the session with ZooKeeper and establish connection
     * @return
     */
    public ZooKeeperSession open() {
        this.client = newClient(connectionString, sessionTimeoutMs, connectionTimeoutMs, new RetryNTimes(numberOfTries, retryTime));
        client.start();
        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return this;
    }


    /**
     * Checks the startup status of the cluster from Apache ZooKeeper
     *
     * @param clusterStatusPath
     * @param fallBackValue
     * @return True if cluster is already marked as started
     */
    public boolean getClusterInitStatus(String clusterStatusPath, boolean fallBackValue) {
        checkConnectivity();
        byte[] data;
        try {
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
        checkConnectivity();
        byte[] data = booleanToBytes(status);
        try {
            client.create().orSetData().creatingParentContainersIfNeeded().forPath(clusterStatusPath, data);
            logger.warn("Cluster starup status is updated in ZooKeeper");
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
        checkConnectivity();
        List<String> nodes = null;
        try {
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
        checkConnectivity();
        byte[] data = null;
        try {
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
        checkConnectivity();
        try {
            if (ttl > 0) {
                client.create().orSetData().withTtl(ttl).creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_WITH_TTL).forPath(nodePath, longToBytes(timeMillis));
            } else {
                client.create().orSetData().creatingParentContainersIfNeeded().forPath(nodePath, longToBytes(timeMillis));
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }


    /**
     * checks if the connection is established with ZooKeeper
     *
     * @throws IllegalStateException
     */
    private void checkConnectivity() throws IllegalStateException {
        Objects.requireNonNull(client);
        if (client.getState() != CuratorFrameworkState.STARTED) {
            throw new IllegalStateException("Client connection is not established.");
        }

    }

    @Override
    /**
     * close connection and session with ZooKeeper
     */
    public void close() {
        if (this.client != null && client.getState() != CuratorFrameworkState.STOPPED) {
            this.client.close();
        }
    }

    public CuratorFramework getClient() {
        return client;
    }
}
