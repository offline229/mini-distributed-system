package com.mds.client.handler;

import org.apache.zookeeper.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperHandler {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperHandler.class);
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    // 修改路径常量
    private static final String ROOT_PATH = "/mds";
    private static final String MASTER_BASE_PATH = ROOT_PATH + "/master";
    private static final String MASTER_ACTIVE_PATH = MASTER_BASE_PATH + "/active";

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
            // 1. 获取活跃master节点列表
            List<String> activeNodes = zooKeeper.getChildren(MASTER_ACTIVE_PATH, false);
            if (activeNodes == null || activeNodes.isEmpty()) {
                logger.warn("没有找到活跃的Master节点");
                return null;
            }

            // 2. 获取活跃节点的信息
            String activeMasterPath = MASTER_ACTIVE_PATH + "/" + activeNodes.get(0);
            byte[] data = zooKeeper.getData(activeMasterPath, false, null);

            if (data != null) {
                String masterInfo = new String(data);
                logger.info("获取到Master信息: {}", masterInfo);
                // 验证数据格式
                JSONObject json = new JSONObject(masterInfo);
                if (!json.has("host") || !json.has("port")) {
                    logger.warn("Master信息格式不正确: {}", masterInfo);
                    return null;
                }
                return masterInfo;
            }

            return null;
        } catch (KeeperException | InterruptedException e) {
            logger.error("获取Master信息失败: {}", e.getMessage());
            throw e;
        }
    }

    // 添加新的辅助方法
    private JSONObject parseMasterInfo(String jsonStr) throws Exception {
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
            return null;
        }
        return new JSONObject(jsonStr);
    }

    public String getMasterHost() throws Exception {
        JSONObject masterInfo = parseMasterInfo(getMasterInfo());
        return masterInfo != null ? masterInfo.getString("host") : null;
    }

    public int getMasterPort() throws Exception {
        JSONObject masterInfo = parseMasterInfo(getMasterInfo());
        return masterInfo != null ? masterInfo.getInt("port") : -1;
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