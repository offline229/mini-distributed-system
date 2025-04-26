package com.example.test;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ZKConnectTest {
    private static final Logger logger = LoggerFactory.getLogger(ZKConnectTest.class);
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 5000;

    @Test
    public void testZKConnection() throws Exception {
        logger.info("开始测试 ZooKeeper 连接...");
        
        CountDownLatch connectedLatch = new CountDownLatch(1);
        
        ZooKeeper zk = new ZooKeeper(ZK_ADDRESS, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    logger.info("成功连接到 ZooKeeper");
                    connectedLatch.countDown();
                }
            }
        });
        
        try {
            connectedLatch.await();
            logger.info("ZooKeeper 连接测试成功");
        } finally {
            zk.close();
        }
    }
} 