package com.mds.region.handler;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZookeeperHandler {
    private static final String ZK_HOST = "localhost:2181";
    private static final int ZK_TIMEOUT = 3000;
    private static final String REGION_PATH = "/regions";
    private static final String ZK_ROOT = "/mds";
    private static final String REGION_SERVER_PATH = ZK_ROOT + "/region-server";

    private ZooKeeper zk;
    private boolean initialized = false; // 新增

    public boolean isInitialized() {
        return initialized && zk != null;
    }
    // 初始化ZooKeeper连接
    public void init() throws IOException {
        if (isInitialized()) return;
        zk = new ZooKeeper(ZK_HOST, ZK_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("ZooKeeper事件：" + event);
            }
        });
        initialized = true;
        System.out.println("ZooKeeper连接成功！");
    }

    // 注销Region节点
    public void unregisterRegionServer(String regionId) throws KeeperException, InterruptedException {
        String path = REGION_PATH + "/" + regionId;
        if (zk.exists(path, false) != null) {
            zk.delete(path, -1);
            System.out.println("Region节点注销成功：" + path);
        }
    }

    public void updateRegionServerData(String serverId, String data) throws KeeperException, InterruptedException {
        // 1. 确保根路径存在
        createIfNotExists(ZK_ROOT);
        createIfNotExists(REGION_SERVER_PATH);

        // 2. 构建完整路径
        String serverPath = REGION_SERVER_PATH + "/" + serverId;
        byte[] dataBytes = data.getBytes();

        // 3. 检查节点是否存在并更新或创建
        if (zk.exists(serverPath, false) == null) {
            // 节点不存在，创建新节点
            try {
                zk.create(serverPath, dataBytes,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                System.out.println("创建RegionServer节点成功：" + serverPath);
            } catch (KeeperException.NodeExistsException e) {
                // 处理并发创建的情况
                zk.setData(serverPath, dataBytes, -1);
            }
        } else {
            // 节点存在，更新数据
            zk.setData(serverPath, dataBytes, -1);
            System.out.println("更新RegionServer数据成功：" + serverPath);
        }
    }

    private void createIfNotExists(String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                System.out.println("创建路径成功：" + path);
            } catch (KeeperException.NodeExistsException e) {
                // 忽略节点已存在的异常
            }
        }
    }

    // 关闭ZooKeeper连接
    public void close() throws InterruptedException {
        if (zk != null) {
            zk.close();
            initialized = false; // 断开时重置
            System.out.println("ZooKeeper连接已关闭！");
        }
    }
}