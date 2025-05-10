package com.mds.client.handler;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;

public class ZookeeperHandler {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperHandler.class);
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;

    private ZooKeeper zooKeeper;

    public void init() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(ZK_ADDRESS, SESSION_TIMEOUT, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedLatch.countDown();
            }
        });
        connectedLatch.await();
        logger.info("Client ZooKeeper连接成功");
    }

    public String getMasterInfo() throws Exception {
        try {
            byte[] data = zooKeeper.getData("/mds/master/active", false, null);
            return data != null ? new String(data) : null;
        } catch (Exception e) {
            logger.error("获取Master信息失败: {}", e.getMessage());
            throw e;
        }
    }

    public void close() {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
                logger.info("Client ZooKeeper连接已关闭");
            } catch (InterruptedException e) {
                logger.error("关闭ZooKeeper连接失败: {}", e.getMessage());
            }
        }
    }
}