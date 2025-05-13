package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionServerInfo;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

import java.util.*;

public class ZKSyncManager {
    private static final String BASE_PATH = "/mds/regions-meta";
    private final ZooKeeper zk;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ZKSyncManager(ZooKeeper zk) {
        this.zk = zk;
        try {
            // 确保基础路径存在
            if (zk.exists(BASE_PATH, false) == null) {
                zk.create(BASE_PATH, new byte[0],
                        org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException("初始化ZK基础路径失败", e);
        }
    }

    public void registerRegion(RegionServerInfo info) throws Exception {
        // 使用regionserverId作为节点路径
        String path = BASE_PATH + "/" + info.getRegionserverID();
        byte[] data = objectMapper.writeValueAsBytes(info);

        try {
            if (zk.exists(path, false) == null) {
                zk.create(path, data,
                        org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                System.out.println("ZK 注册 Region 节点：" + path);
            } else {
                zk.setData(path, data, -1);
                System.out.println("ZK 更新 Region 节点：" + path);
            }
        } catch (Exception e) {
            System.err.println("注册Region失败: " + e.getMessage());
            throw e;
        }
    }

    public List<RegionServerInfo> getAllRegions() throws Exception {
        List<RegionServerInfo> regionList = new ArrayList<>();
        try {
            List<String> children = zk.getChildren(BASE_PATH, false);
            for (String child : children) {
                String path = BASE_PATH + "/" + child;
                byte[] data = zk.getData(path, false, null);
                RegionServerInfo info = objectMapper.readValue(data, RegionServerInfo.class);
                regionList.add(info);
            }
        } catch (Exception e) {
            System.err.println("获取所有Region失败: " + e.getMessage());
            throw e;
        }
        return regionList;
    }

    public void updateRegionInfo(RegionServerInfo info) throws Exception {
        String path = BASE_PATH + "/" + info.getRegionserverID();
        byte[] data = objectMapper.writeValueAsBytes(info);
        try {
            if (zk.exists(path, false) != null) {
                zk.setData(path, data, -1);
                System.out.println("ZK 更新 Region 数据：" + path);
            } else {
                System.err.println("尝试更新不存在的 Region 节点：" + path);
                // 如果节点不存在，重新注册
                registerRegion(info);
            }
        } catch (Exception e) {
            System.err.println("更新Region信息失败: " + e.getMessage());
            throw e;
        }
    }

    public void unregisterRegion(String regionserverId) throws Exception {
        String path = BASE_PATH + "/" + regionserverId;
        try {
            if (zk.exists(path, false) != null) {
                zk.delete(path, -1);
                System.out.println("ZK 删除 Region 节点：" + path);
            }
        } catch (Exception e) {
            System.err.println("注销Region失败: " + e.getMessage());
            throw e;
        }
    }

    public RegionServerInfo getRegion(String regionserverId) throws Exception {
        String path = BASE_PATH + "/" + regionserverId;
        try {
            if (zk.exists(path, false) == null)
                return null;
            byte[] data = zk.getData(path, false, null);
            return objectMapper.readValue(data, RegionServerInfo.class);
        } catch (Exception e) {
            System.err.println("获取Region信息失败: " + e.getMessage());
            throw e;
        }
    }
}