package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionInfo;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

public class ZKSyncManager {
    private static final String BASE_PATH = "/regions";
    private final ZooKeeper zk;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ZKSyncManager(ZooKeeper zk) {
        this.zk = zk;
    }

    public void registerRegion(RegionInfo info) throws Exception {
        String path = BASE_PATH + "/" + info.getRegionId();
        byte[] data = objectMapper.writeValueAsBytes(info);

        if (zk.exists(BASE_PATH, false) == null) {
            zk.create(BASE_PATH, new byte[0],
                    org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }

        // 临时节点代表心跳
        zk.create(path, data,
                org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        System.out.println("ZK 注册 Region 节点：" + path);
    }
}
