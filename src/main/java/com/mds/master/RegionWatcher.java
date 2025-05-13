package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionServerInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class RegionWatcher {
    private static final String BASE_PATH = "/regions";
    private final CuratorFramework client;
    private final Map<String, RegionServerInfo> onlineRegions = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private PathChildrenCache cache;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public RegionWatcher(CuratorFramework client) {
        this.client = client;
    }

    public void startWatching() throws Exception {
        if (!isRunning.compareAndSet(false, true)) {
            System.out.println("[RegionWatcher] 监听器已在运行中");
            return;
        }

        try {
            // 确保基础路径存在
            if (client.checkExists().forPath(BASE_PATH) == null) {
                client.create()
                    .creatingParentsIfNeeded()
                    .forPath(BASE_PATH);
            }

            cache = new PathChildrenCache(client, BASE_PATH, true);
            cache.getListenable().addListener(createCacheListener());
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

            System.out.println("[RegionWatcher] 已启动监听器，初始 Region 数量：" + onlineRegions.size());
        } catch (Exception e) {
            isRunning.set(false);
            throw new RuntimeException("[RegionWatcher] 启动失败: " + e.getMessage(), e);
        }
    }

    public void stop() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }

        try {
            if (cache != null) {
                cache.close();
                cache = null;
            }
            onlineRegions.clear();
            System.out.println("[RegionWatcher] 已停止监听器");
        } catch (Exception e) {
            System.err.println("[RegionWatcher] 停止监听器时发生错误: " + e.getMessage());
        }
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public Map<String, RegionServerInfo> getOnlineRegions() {
        return onlineRegions;
    }

    public RegionServerInfo getRegionById(String regionserverId) {
        if (!isRunning.get()) {
            System.err.println("[RegionWatcher] 监听器未运行");
            return null;
        }
        return onlineRegions.get(regionserverId);
    }

    public RegionServerInfo findRegionServerByReplicaKey(String replicaKey) {
        if (!isRunning.get() || replicaKey == null) {
            return null;
        }
        return onlineRegions.values().stream()
                .filter(info -> replicaKey.equals(info.getReplicaKey()))
                .findFirst()
                .orElse(null);
    }

    public RegionServerInfo findLeastLoadedRegionServer() {
        if (!isRunning.get()) {
            return null;
        }
        return onlineRegions.values().stream()
                .min((r1, r2) -> {
                    int load1 = calculateLoad(r1);
                    int load2 = calculateLoad(r2);
                    return Integer.compare(load1, load2);
                })
                .orElse(null);
    }

    private int calculateLoad(RegionServerInfo region) {
        return region.getHostsPortsStatusList().stream()
                .mapToInt(RegionServerInfo.HostPortStatus::getConnections)
                .sum();
    }

    private PathChildrenCacheListener createCacheListener() {
        return (client, event) -> {
            if (!isRunning.get()) return;

            try {
                String path = event.getData() != null ? event.getData().getPath() : null;
                if (path == null) return;

                switch (event.getType()) {
                    case CHILD_ADDED -> handleChildAdded(path, event.getData().getData());
                    case CHILD_UPDATED -> handleChildUpdated(path, event.getData().getData());
                    case CHILD_REMOVED -> handleChildRemoved(path);
                    case CONNECTION_LOST -> handleConnectionLost();
                    case CONNECTION_RECONNECTED -> handleReconnected();
                    default -> {
                        // 忽略其他事件
                    }
                }
            } catch (Exception e) {
                System.err.println("[RegionWatcher] 处理事件失败: " + e.getMessage());
            }
        };
    }

    private void handleChildAdded(String path, byte[] data) {
        try {
            RegionServerInfo info = objectMapper.readValue(data, RegionServerInfo.class);
            onlineRegions.put(info.getRegionserverID(), info);
            System.out.println("[RegionWatcher] 节点上线：" + info.getRegionserverID());
        } catch (Exception e) {
            System.err.println("[RegionWatcher] 解析 CHILD_ADDED 事件失败：" + e.getMessage());
        }
    }

    private void handleChildUpdated(String path, byte[] data) {
        try {
            RegionServerInfo info = objectMapper.readValue(data, RegionServerInfo.class);
            onlineRegions.put(info.getRegionserverID(), info);
            System.out.println("[RegionWatcher] 节点更新：" + info.getRegionserverID());
        } catch (Exception e) {
            System.err.println("[RegionWatcher] 解析 CHILD_UPDATED 事件失败：" + e.getMessage());
        }
    }

    private void handleChildRemoved(String path) {
        String regionserverId = extractRegionServerId(path);
        RegionServerInfo removed = onlineRegions.remove(regionserverId);
        if (removed != null) {
            System.out.println("[RegionWatcher] 节点下线：" + regionserverId);
        }
    }

    private void handleConnectionLost() {
        System.err.println("[RegionWatcher] 与ZooKeeper的连接已断开");
    }

    private void handleReconnected() {
        System.out.println("[RegionWatcher] 已重新连接到ZooKeeper");
        try {
            refreshCache();
        } catch (Exception e) {
            System.err.println("[RegionWatcher] 刷新缓存失败: " + e.getMessage());
        }
    }

    private void refreshCache() throws Exception {
        onlineRegions.clear();
        if (cache != null) {
            cache.rebuild();
        }
    }

    private String extractRegionServerId(String path) {
        return path.substring(path.lastIndexOf("/") + 1);
    }
}