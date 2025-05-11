package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RegionWatcher {
    private static final String BASE_PATH = "/regions";
    private final CuratorFramework client;
    //存储当前所有在线 Region 节点的信息，key 为 Region ID，value 为Region=host+port
    //ConcurrentHashMap 是为了支持多线程并发访问（因为监听器是异步触发的）

    private final Map<String, RegionInfo> onlineRegions = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private PathChildrenCache cache;

//    private static final long HEARTBEAT_TIMEOUT = 15_000; // 15秒未更新视为下线
//    private static final long CHECK_INTERVAL = 10_000; // 每10秒检查一次

    public RegionWatcher(CuratorFramework client) {
        this.client = client;
    }

    public void startWatching() throws Exception {
        cache = new PathChildrenCache(client, BASE_PATH, true);
        cache.getListenable().addListener((client, event) -> {
            String path = event.getData() != null ? event.getData().getPath() : "null";

            switch (event.getType()) {
                case CHILD_ADDED -> {
                    RegionInfo info = parseRegionInfo(event);
                    if (info != null) {
                        onlineRegions.put(info.getRegionId(), info);
                        System.out.println("[RegionWatcher] 节点上线：" + info);
                    }
                }
                case CHILD_UPDATED -> {
                    RegionInfo info = parseRegionInfo(event);
                    if (info != null) {
                        onlineRegions.put(info.getRegionId(), info);
                        System.out.println("[RegionWatcher] 节点更新：" + info);
                    }
                }
                case CHILD_REMOVED -> {
                    String regionId = extractRegionId(path);
                    onlineRegions.remove(regionId);
                    System.out.println("[RegionWatcher] 节点下线：" + regionId);
                }
                default -> {
                    // 忽略其他事件
                }
            }
        });
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        System.out.println("[RegionWatcher] 已启动监听器，初始 Region 数量：" + onlineRegions.size());
    }

    public void stop() throws Exception {
        if (cache != null) {
            cache.close();
        }
    }

    public Map<String, RegionInfo> getOnlineRegions() {
        return onlineRegions;
    }

    public RegionInfo getRegionById(String regionId) {
        return onlineRegions.get(regionId);
    }

    private RegionInfo parseRegionInfo(PathChildrenCacheEvent event) {
        try {
            byte[] data = event.getData().getData();
            return objectMapper.readValue(data, RegionInfo.class);
        } catch (Exception e) {
            System.err.println("[RegionWatcher] 解析 RegionInfo 失败：" + e.getMessage());
            return null;
        }
    }

    private String extractRegionId(String path) {
        return path.substring(path.lastIndexOf("/") + 1);
    }
}


