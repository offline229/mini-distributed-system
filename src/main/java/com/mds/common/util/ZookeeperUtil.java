package com.mds.common.util;

import com.mds.common.config.SystemConfig;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperUtil.class);
    private ZooKeeper zk;
    private CountDownLatch connectedLatch = new CountDownLatch(1);

    public ZookeeperUtil() throws IOException, InterruptedException {
        connect();
    }

    private void connect() throws IOException, InterruptedException {
        zk = new ZooKeeper(SystemConfig.ZK_CONNECT_STRING, SystemConfig.ZK_SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectedLatch.countDown();
                }
            }
        });
        connectedLatch.await();
        initRootPath();
    }

    private void initRootPath() {
        try {
            createPath(SystemConfig.ZK_ROOT_PATH, CreateMode.PERSISTENT);
            createPath(SystemConfig.ZK_REGION_PATH, CreateMode.PERSISTENT);
            createPath(SystemConfig.ZK_MASTER_PATH, CreateMode.PERSISTENT);
            createPath(SystemConfig.ZK_TABLE_PATH, CreateMode.PERSISTENT);
        } catch (Exception e) {
            logger.error("初始化ZooKeeper路径失败", e);
        }
    }

    public void createPath(String path, CreateMode mode) throws KeeperException, InterruptedException {
        if (exists(path)) {
            return;
        }
        try {
            zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        } catch (KeeperException.NodeExistsException e) {
            logger.warn("节点已存在: {}", path);
        }
    }

    public boolean exists(String path) throws KeeperException, InterruptedException {
        return zk.exists(path, false) != null;
    }

    public void setData(String path, byte[] data) throws KeeperException, InterruptedException {
        if (exists(path)) {
            zk.setData(path, data, -1);
        } else {
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public byte[] getData(String path) throws KeeperException, InterruptedException {
        if (!exists(path)) {
            return null;
        }
        return zk.getData(path, false, null);
    }

    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        if (!exists(path)) {
            return new ArrayList<>();
        }
        return zk.getChildren(path, false);
    }

    public void delete(String path) throws KeeperException, InterruptedException {
        if (exists(path)) {
            zk.delete(path, -1);
        }
    }

    public void close() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }
} 