package com.hazelnut.cluster;

import com.hazelnut.utils.ZkConnectionManager;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Service
/**
 * Distributed lock is a wrapper service over InterProcessMutex from curator framework
 * It provides the cluster level locking using Apache ZooKeeper locking service
 */
public class DistributedLock implements DisposableBean {

    @Value("${distributed.lock.path}")
    private String lockPath;

    @Value("${distributed.lock.timeout.ms}")
    private long timeoutMillis;

    private ZkConnectionManager zkConnectionManager;

    InterProcessMutex lock = null;

    private Logger logger = LoggerFactory.getLogger(DistributedLock.class);

    public DistributedLock(@Autowired ZkConnectionManager zkConnectionManager) {
        this.zkConnectionManager = zkConnectionManager;

    }

    /**
     * try acquiring the distributed lock
     * try times out as per configured time
     */
    public boolean tryLock() {
        try {
            lock = lock == null ? new InterProcessMutex(zkConnectionManager.activeConnection(), lockPath) : lock;
            return lock.isAcquiredInThisProcess() || lock.acquire(timeoutMillis, MILLISECONDS);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return false;
    }

    /**
     * release the distributed lock
     */
    public void releaseLock() {
        if (lock != null && lock.isAcquiredInThisProcess()) {
            try {
                lock.release();
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        releaseLock();
    }


}
