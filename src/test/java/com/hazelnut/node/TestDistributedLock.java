package com.hazelnut.node;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.hazelnut.cluster.DistributedLock;
import com.hazelnut.node.preps.DistributedLockTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.stream.Stream;

@SpringBootTest
@RunWith(SpringRunner.class)
@ExtendWith(SpringExtension.class)
class TestDistributedLock extends DistributedLockTestUtils {

    @Autowired
    protected DistributedLock lock;

    @BeforeEach
    public void setUp() {
        ReflectionTestUtils.setField(lock, "lock", mutex);
    }

    @Test
    void testAcquiringLock() {
        lock.tryLock();
        verifyDistributedLockIsTried(EXACTLY_ONCE);
    }

    @Test
    void testAcquiringAlreadyAcquiredLock() {
        mockThatLockIsAlreadyAcquired();
        lock.tryLock();
        verifyDistributedLockIsTried(NEVER);
    }

    @Test
    void testReleasingTheLock() {
        mockThatLockIsAlreadyAcquired();
        lock.releaseIfLocked();
        verifyReleaseLockCallIsMade(EXACTLY_ONCE);
    }

    @Test
    void testReleasingTheLockNotAcquired() {
        lock.releaseIfLocked();
        verifyReleaseLockCallIsMade(NEVER);
    }


    @Configuration
    @Import(DistributedLock.class)
    static class Config {
    }

}
