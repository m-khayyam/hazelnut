package com.hazelnut.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;

import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;

@Service
public class ZkClientSupplier {
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

    private final Supplier<CuratorFramework> supplier;

    public ZkClientSupplier() {
        this.supplier = () -> newClient(connectionString, sessionTimeoutMs, connectionTimeoutMs, new RetryNTimes(numberOfTries, retryTime));

    }

    /***
     * Establish and Returns a new and active client for ZooKeeper connectivity
     * Caller should be responsible for the client end of job
     * @return a zookeeper client
     */
    public CuratorFramework newZkClient() {
        CuratorFramework client = supplier.get();
        client.start();

        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return client;
    }

}
