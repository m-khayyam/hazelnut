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
    private int retryTimeMs;

    @Value("${client.retry.attempts.count}")
    private int numberOfTries;

    private final Logger logger = LoggerFactory.getLogger(ZooKeeperSession.class);

    private CuratorFramework client;


    /**
     * Open the session with ZooKeeper and establish connection
     *
     * @return
     */
    public ZooKeeperSession open() {
        this.client = newClient(connectionString, sessionTimeoutMs, connectionTimeoutMs, new RetryNTimes(numberOfTries, retryTimeMs));
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
     * @return True if cluster is already marked as started
     */
    public boolean getClusterStatus(String clusterStatusPath) {
        checkConnectivity();
        boolean status = false;
        try {
            byte[] data = client.getData().forPath(clusterStatusPath);
            status = data != null && bytesToBoolean(data);
        } catch (KeeperException.NoNodeException e) {
            logger.info("Cluster status never reported. Thus unable to fetch status.");
            logger.warn(e.getMessage());

        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return status;
    }

    /**
     * report the heart beat of this running node to ZooKeeper
     *
     * @param nodePath
     * @param ttl      i.e. time to live for this data, after this time the cluster will be considered inactive
     */
    public void markClusterAsActive(String nodePath, long ttl) {
        checkConnectivity();
        try {
            if (ttl > 0) {
                client.create().orSetData().withTtl(ttl).creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_WITH_TTL).forPath(nodePath, booleanToBytes(true));
            } else {
                client.create().orSetData().creatingParentContainersIfNeeded().forPath(nodePath, booleanToBytes(true));
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
