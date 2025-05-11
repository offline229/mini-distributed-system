package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionInfo;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

import java.util.*;

public class ZKSyncManager {
    private static final String BASE_PATH = "/regions";
    private final ZooKeeper zk;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ZKSyncManager(ZooKeeper zk) {
        this.zk = zk;
    }

    //注册region节点（临时节点表示存活）
    public void registerRegion(RegionInfo info) throws Exception {
        String path = BASE_PATH + "/" + info.getRegionId();
        byte[] data = objectMapper.writeValueAsBytes(info);

        if (zk.exists(BASE_PATH, false) == null) {
            zk.create(BASE_PATH, new byte[0],
                    org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }

        // 临时节点代表心跳，避免重复注册
        if (zk.exists(path, false) == null) {
            zk.create(path, data,
                    org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            System.out.println("ZK 注册 Region 节点：" + path);
        } else {
            zk.setData(path, data, -1);  // 更新数据
            System.out.println("ZK 已存在 Region，更新数据：" + path);
        }
    }

    //获取所有在线region节点
    public List<RegionInfo> getAllRegions() throws Exception {
        List<RegionInfo> regionList = new ArrayList<>();
        if (zk.exists(BASE_PATH, false) == null) return regionList;

        List<String> children = zk.getChildren(BASE_PATH, false);
        for (String child : children) {
            String path = BASE_PATH + "/" + child;
            byte[] data = zk.getData(path, false, null);
            RegionInfo info = objectMapper.readValue(data, RegionInfo.class);
            regionList.add(info);
        }
        return regionList;
    }

    //更新节点数据
    //？：只更新负载的话是可以用meta里的函数
    public void updateRegionInfo(RegionInfo info) throws Exception {
        String path = BASE_PATH + "/" + info.getRegionId();
        byte[] data = objectMapper.writeValueAsBytes(info);
        if (zk.exists(path, false) != null) {
            zk.setData(path, data, -1);
        } else {
            System.err.println("尝试更新不存在的 Region 节点：" + path);
        }
    }

    //注销节点
    public void unregisterRegion(String regionId) throws Exception {
        String path = BASE_PATH + "/" + regionId;
        if (zk.exists(path, false) != null) {
            zk.delete(path, -1);
            System.out.println("ZK 删除 Region 节点：" + path);
        }
    }

    //取指定 Region 节点信息（用于调度时的读取）
    public RegionInfo getRegion(String regionId) throws Exception {
        String path = BASE_PATH + "/" + regionId;
        if (zk.exists(path, false) == null) return null;
        byte[] data = zk.getData(path, false, null);
        return objectMapper.readValue(data, RegionInfo.class);
    }
}
