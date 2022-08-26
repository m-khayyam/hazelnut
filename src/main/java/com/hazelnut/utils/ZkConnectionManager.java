package com.hazelnut.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;

@Service
public class ZkConnectionManager implements DisposableBean {
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

    private Supplier<CuratorFramework> supplier = null;

    private CuratorFramework connection;

    private Map<String, InterProcessMutex> locksMap = new HashMap<>();

    private Logger logger = LoggerFactory.getLogger(ZkConnectionManager.class);


    public ZkConnectionManager() {
        this.supplier = () -> newClient(connectionString, sessionTimeoutMs, connectionTimeoutMs, new RetryNTimes(numberOfTries, retryTime));

    }

    /***
     * Returns already active or a new and active connection for ZooKeeper connectivity
     * @return
     */
    public CuratorFramework activeConnection() {
        if (connection == null || connection.getState() == CuratorFrameworkState.STOPPED) {
            connection = supplier.get();
        }

        if (connection.getState() == CuratorFrameworkState.LATENT) {
            connection.start();
        }
        try {
            connection.blockUntilConnected();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return connection;
    }

    /**
     * close the connection in hand and provides a new one
     * Use this method instead of activeConnection when connection in hand is in illegal state
     *
     * @return
     */
    public CuratorFramework resetConnection() {
        connection.close();
        return activeConnection();
    }


    /***
     * Distributed lock for inter process synchronization.
     * Please note its reentrant within a process.
     * @param timeoutMillis
     * @param lockPath
     * @return True if lock is acquired otherwise false
     */
    public boolean acquireDistributedLock(Long timeoutMillis, String lockPath) {

        InterProcessMutex lock = locksMap.computeIfAbsent(lockPath, path -> new InterProcessMutex(activeConnection(), path));
        if (lock.isAcquiredInThisProcess()) {
            return true;
        }
        try {
            return lock.acquire(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return false;
    }


    /**
     * release the lock for given path/key
     *
     * @param lockPath
     */
    public void releaseLock(String lockPath) {


        InterProcessMutex lock = locksMap.get(lockPath);
        if (lock != null && lock.isAcquiredInThisProcess()) {
            try {
                lock.release();
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    /**
     * release all locks acquired by the process
     */
    private void releaseAllLock() {
        locksMap.values().stream().filter(l -> l != null && l.isAcquiredInThisProcess()).forEach(l -> {
            try {
                l.release();
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }

        });
    }


    @Override
    public void destroy() throws Exception {
        releaseAllLock();
        connection.close();
    }
}
