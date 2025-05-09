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

    private ZooKeeper zk;

    // 初始化ZooKeeper连接
    public void init() throws IOException {
        zk = new ZooKeeper(ZK_HOST, ZK_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("ZooKeeper事件：" + event);
            }
        });
        System.out.println("ZooKeeper连接成功！");
    }

    // 注册Region节点
    public void registerRegion(String regionId, String regionData) throws KeeperException, InterruptedException {
        String path = REGION_PATH + "/" + regionId;
        if (zk.exists(REGION_PATH, false) == null) {
            zk.create(REGION_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); // 修复权限设置
        }
        zk.create(path, regionData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL); // 修复权限设置
        System.out.println("Region节点注册成功：" + path);
    }

    // 注销Region节点
    public void unregisterRegion(String regionId) throws KeeperException, InterruptedException {
        String path = REGION_PATH + "/" + regionId;
        if (zk.exists(path, false) != null) {
            zk.delete(path, -1);
            System.out.println("Region节点注销成功：" + path);
        }
    }

    // 关闭ZooKeeper连接
    public void close() throws InterruptedException {
        if (zk != null) {
            zk.close();
            System.out.println("ZooKeeper连接已关闭！");
        }
    }
}