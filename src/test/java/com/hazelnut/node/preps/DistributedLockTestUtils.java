package com.hazelnut.node.preps;

import com.hazelnut.cluster.ZooKeeperSession;
import com.hazelnut.node.NodeLivenessReporter;
import com.hazelnut.node.NodeStartup;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class DistributedLockTestUtils extends CommonTestUtils {

    @MockBean
    protected ZooKeeperSession zooKeeperSession;

    @MockBean
    protected NodeStartup nodeStartup;

    @MockBean
    protected NodeLivenessReporter reporter;

    protected InterProcessMutex mutex = mock(InterProcessMutex.class);

    protected void mockThatLockIsAlreadyAcquired() {
            Mockito.when(mutex.isAcquiredInThisProcess()).thenReturn(true);
    }

    protected void verifyDistributedLockIsTried(int count) {
        try {
            Mockito.verify(mutex, times(count)).acquire(anyLong(), any(TimeUnit.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void verifyReleaseLockCallIsMade(int count) {
        try {
            Mockito.verify(mutex, times(count)).release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
