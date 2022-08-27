package com.hazelnut.cluster;

import com.hazelnut.utils.ZkClientSupplier;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.Closeable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Service
/**
 * Distributed lock is a wrapper service over InterProcessMutex from curator framework
 * It provides the cluster level locking using Apache ZooKeeper locking service
 */
public class DistributedLock implements Closeable {

    @Value("${distributed.lock.path}")
    private String lockPath;

    @Value("${distributed.lock.timeout.ms}")
    private long timeoutMillis;

    private final ZkClientSupplier zkClientSupplier;

    InterProcessMutex lock = null;

    CuratorFramework client;
    private final Logger logger = LoggerFactory.getLogger(DistributedLock.class);

    public DistributedLock(@Autowired ZkClientSupplier zkClientSupplier) {
        this.zkClientSupplier = zkClientSupplier;

    }

    /**
     * try acquiring the distributed lock
     * try times out as per configured time
     * DistributedLock is reentrant
     * trylock is idempodent
     * lock needs zookeeper connection. connection is not closeable in same block, but after releasing lock
     */
    public DistributedLock tryLock() {
        if (client == null || client.getState() == CuratorFrameworkState.STOPPED) {
            client = zkClientSupplier.newZkClient();
        }
        if (lock == null) {
            lock = new InterProcessMutex(client, lockPath);
        }
        if (!lock.isAcquiredInThisProcess()) {
            try {
                lock.acquire(timeoutMillis, MILLISECONDS);
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }

        return this;
    }

    /**
     * release the distributed lock
     * close the client connection used for lock
     */
    public void releaseIfLocked() {

        if (lock != null && lock.isAcquiredInThisProcess()) {
            try {
                lock.release();
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
            if (client != null) {
                client.close();
            }
        }
    }

    @Override
    /**
     * auto release the lock in try with resource block
     */
    public void close() {
        releaseIfLocked();
    }

}
