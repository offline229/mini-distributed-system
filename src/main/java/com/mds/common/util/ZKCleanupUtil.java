package com.mds.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKCleanupUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZKCleanupUtil.class);
    private static final String MDS_ROOT = "/mds";

    public static void cleanupMDSNodes() {
        try {
            logger.info("开始清理 ZooKeeper 中的 MDS 节点...");
            ZookeeperUtil zkUtil = new ZookeeperUtil();
            deleteNodeRecursively(zkUtil, MDS_ROOT);
            logger.info("MDS 节点清理完成");
            zkUtil.close();
        } catch (Exception e) {
            logger.error("清理 MDS 节点失败", e);
        }
    }

    private static void deleteNodeRecursively(ZookeeperUtil zkUtil, String path) throws Exception {
        // 如果节点不存在，直接返回
        if (!zkUtil.exists(path)) {
            logger.info("节点不存在: {}", path);
            return;
        }

        // 获取所有子节点
        for (String child : zkUtil.getChildren(path)) {
            String childPath = path + "/" + child;
            deleteNodeRecursively(zkUtil, childPath);
        }

        // 删除当前节点
        try {
            zkUtil.delete(path);
            logger.info("成功删除节点: {}", path);
        } catch (Exception e) {
            logger.error("删除节点失败: {}", path, e);
            throw e;
        }
    }

    public static void main(String[] args) {
        cleanupMDSNodes();
    }
}