package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionServerInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RegionWatcher {
    // 监听器的基础路径
    private static final String BASE_PATH = "/mds/regions-meta";
    // 心跳超时的时间间隔——30秒超时
    private static final long HEARTBEAT_TIMEOUT = 30_000;
    // 存储在线的 RegionServer 信息
    private final Map<String, RegionServerInfo> onlineRegions = new ConcurrentHashMap<>();
    private PathChildrenCache cache;    // 监听器缓存
    private final AtomicBoolean isRunning = new AtomicBoolean(false);   // 监听器是否正在运行
    // 定时任务调度器
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final CuratorFramework client;  // ZooKeeper 客户端
    private final ObjectMapper objectMapper = new ObjectMapper();   // JSON 解析器

    // 构造函数
    public RegionWatcher(CuratorFramework client) {
        this.client = client;
    }

    // 启动监听器
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

            // 创建 PathChildrenCache 监听器
            cache = new PathChildrenCache(client, BASE_PATH, true);
            // 设置数据缓存
            cache.getListenable().addListener(createCacheListener());
            // 启动监听器
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            // 启动心跳超时检查任务
            scheduler.scheduleAtFixedRate(this::checkHeartbeatTimeout, HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS);

            System.out.println("[RegionWatcher] 已启动监听器，初始 Region 数量：" + onlineRegions.size());
        } catch (Exception e) {
            isRunning.set(false);   // 启动失败时重置状态
            throw new RuntimeException("[RegionWatcher] 启动失败: " + e.getMessage(), e);
        }
    }

    // 停止监听器
    public void stop() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }

        try {
            scheduler.shutdownNow(); // 停止定时任务
            if (cache != null) {
                cache.close();  // 关闭监听器
                cache = null;   // 清空引用
            }
            onlineRegions.clear();  // 清空在线 RegionServer 信息
            System.out.println("[RegionWatcher] 已停止监听器");
        } catch (Exception e) {
            System.err.println("[RegionWatcher] 停止监听器时发生错误: " + e.getMessage());
        }
    }

    // 获取当前 RegionWatcher 是否在运行
    public boolean isRunning() {
        return isRunning.get();
    }

    // 获取在线的 RegionServer 信息
    public Map<String, RegionServerInfo> getOnlineRegions() {
        return onlineRegions;
    }

    // 根据regionserverId获取所有在线的 RegionServer
    public RegionServerInfo getRegionById(String regionserverId) {
        return onlineRegions.get(regionserverId);
    }

    // 根据副本key获取所有在线的 RegionServer
    public RegionServerInfo findRegionServerByReplicaKey(String replicaKey) {
        return onlineRegions.values().stream()
                .filter(region -> replicaKey.equals(region.getReplicaKey()))
                .findFirst()
                .orElse(null);
    }

    // 检查心跳超时
    public void checkHeartbeatTimeout() {
        if (!isRunning.get()) return;

        long currentTime = System.currentTimeMillis();
        onlineRegions.values().forEach(region -> {
            region.getHostsPortsStatusList().removeIf(hp -> {
                if (currentTime - hp.getLastHeartbeatTime() > HEARTBEAT_TIMEOUT) {
                    System.out.println("[RegionWatcher] 检测到超时 HostPort，下线：" + hp.getHost() + ":" + hp.getPort());
                    return true;
                }
                return false;
            });
        });

        // 移除没有任何 HostPort 的 RegionServer
        onlineRegions.entrySet().removeIf(entry -> {
            if (entry.getValue().getHostsPortsStatusList().isEmpty()) {
                System.out.println("[RegionWatcher] RegionServer 下线：" + entry.getKey());
                return true;
            }
            return false;
        });
    }

    // 创建 PathChildrenCache 监听器
    private PathChildrenCacheListener createCacheListener() {
        return (client, event) -> {
            if (!isRunning.get()) return;

            try {
                String path = event.getData() != null ? event.getData().getPath() : null;
                if (path == null) return;

                // 处理不同类型的事件
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

    // 处理 CHILD_ADDED 事件
    private void handleChildAdded(String path, byte[] data) {
        try {
            RegionServerInfo info = objectMapper.readValue(data, RegionServerInfo.class);
            onlineRegions.put(info.getRegionserverID(), info);
            System.out.println("[RegionWatcher] 节点上线：" + info.getRegionserverID());
        } catch (Exception e) {
            System.err.println("[RegionWatcher] 解析 CHILD_ADDED 事件失败：" + e.getMessage());
        }
    }

    // 处理 CHILD_UPDATED 事件
    private void handleChildUpdated(String path, byte[] data) {
        try {
            RegionServerInfo updatedInfo = objectMapper.readValue(data, RegionServerInfo.class);
            RegionServerInfo existingInfo = onlineRegions.get(updatedInfo.getRegionserverID());
            if (existingInfo != null) {
                // 更新 HostPort 的心跳时间
                updatedInfo.getHostsPortsStatusList().forEach(updatedHostPort -> {
                    existingInfo.getHostsPortsStatusList().stream()
                        .filter(hp -> hp.getHost().equals(updatedHostPort.getHost()) && hp.getPort() == updatedHostPort.getPort())
                        .forEach(hp -> hp.setLastHeartbeatTime(System.currentTimeMillis()));
                });
            } else {
                onlineRegions.put(updatedInfo.getRegionserverID(), updatedInfo);
            }
            System.out.println("[RegionWatcher] 节点更新：" + updatedInfo.getRegionserverID());
        } catch (Exception e) {
            System.err.println("[RegionWatcher] 解析 CHILD_UPDATED 事件失败：" + e.getMessage());
        }
    }

    // 处理 CHILD_REMOVED 事件
    private void handleChildRemoved(String path) {
        String regionserverId = extractRegionServerId(path);
        RegionServerInfo removed = onlineRegions.remove(regionserverId);
        if (removed != null) {
            System.out.println("[RegionWatcher] 节点下线：" + regionserverId);
        }
    }

    // 处理连接丢失事件
    private void handleConnectionLost() {
        System.err.println("[RegionWatcher] 与ZooKeeper的连接已断开");
    }

    // 处理重新连接事件
    private void handleReconnected() {
        System.out.println("[RegionWatcher] 已重新连接到ZooKeeper");
        try {
            refreshCache();
        } catch (Exception e) {
            System.err.println("[RegionWatcher] 刷新缓存失败: " + e.getMessage());
        }
    }

    // 刷新缓存
    private void refreshCache() throws Exception {
        onlineRegions.clear();
        if (cache != null) {
            cache.rebuild();
        }
    }

    // 提取 RegionServer ID
    private String extractRegionServerId(String path) {
        return path.substring(path.lastIndexOf("/") + 1);
    }

}