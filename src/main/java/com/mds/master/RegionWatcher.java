package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionInfo;
import com.mds.region.Region;
import org.apache.curator.framework.*;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RegionWatcher {
    private static final Logger logger = LoggerFactory.getLogger(RegionWatcher.class);
    private static final String REGIONS_PATH = "/regions";
    private static final long HEARTBEAT_TIMEOUT = 15_000; // 15秒未更新视为下线
    private static final long CHECK_INTERVAL = 10_000; // 每10秒检查一次

    private final CuratorFramework client;
    //存储当前所有在线 Region 节点的信息，key 为 Region ID，value 为Region=host+port
    //ConcurrentHashMap 是为了支持多线程并发访问（因为监听器是异步触发的）
    private final Map<String, RegionInfo> onlineRegions=new ConcurrentHashMap<>();
    private final Map<String, Long> lastHeartbeatMap = new ConcurrentHashMap<>();

    private PathChildrenCache cache;
    private ScheduledExecutorService scheduler;

    public RegionWatcher(CuratorFramework client)
    {
        this.client = client;
    }

    public void startWatching() throws Exception
    {
        logger.info("启动 RegionWatcher，监听路径: {}", REGIONS_PATH);
        //初始化Region根路径 如果不存在
        try
        {
            if(client.checkExists().forPath(REGIONS_PATH) == null) {
                client.create().creatingParentsIfNeeded().forPath(REGIONS_PATH);
                logger.info("已创建路径: {}", REGIONS_PATH);
            }
        }catch (KeeperException.NodeExistsException e) {

        }

        //启动缓存监听
        cache = new PathChildrenCache(client, REGIONS_PATH, true);
        cache.getListenable().addListener((c, event) -> {
            String nodePath = event.getData()!=null ? event.getData().getPath() : "null";
            RegionInfo info = parseRegionInfo(event.getData().getData());
            switch (event.getType()) {
                case CHILD_ADDED -> {
                    if (info != null) {
                        onlineRegions.put(info.getRegionId(), info);
                        lastHeartbeatMap.put(info.getRegionId(), System.currentTimeMillis());
                        logger.info("Region 上线: {} -> {}", info.getRegionId(), info);
                    }
                }
                case CHILD_REMOVED -> {
                    onlineRegions.remove(info.getRegionId());
                    lastHeartbeatMap.remove(info.getRegionId());
                    logger.warn("Region 下线: {}", info.getRegionId());
                }
                case CHILD_UPDATED -> {
                    if (info != null) {
                        onlineRegions.put(info.getRegionId(), info);
                        lastHeartbeatMap.put(info.getRegionId(), System.currentTimeMillis());
                        logger.debug("Region 心跳更新: {} -> {}", info.getRegionId(), info);
                    }
                }
                default -> logger.debug("事件: {}", event.getType());
            }
        });
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

        // 初始化加载当前 Region 列表
        for (ChildData child : cache.getCurrentData()) {
            String regionId = extractNodeId(child.getPath());
            RegionInfo info = parseRegionInfo(child.getData());
            if (info != null) {
                onlineRegions.put(regionId, info);
                lastHeartbeatMap.put(regionId, System.currentTimeMillis());
                logger.info("加载 Region: {} -> {}", regionId, info);
            }
        }

       // 启动定时心跳检查任务
       scheduler = Executors.newSingleThreadScheduledExecutor();
       scheduler.scheduleAtFixedRate(this::checkHeartbeats, CHECK_INTERVAL, CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private String extractNodeId(String path) {
        return path == null ? "unknown" : path.substring(path.lastIndexOf('/') + 1);
    }

    private RegionInfo parseRegionInfo(byte[] data) {
        try {
            return objectMapper.readValue(data, RegionInfo.class);
        } catch (Exception e) {
            logger.error("解析 RegionInfo 失败: {}", new String(data), e);
            return null;
        }
    }

    //获取在线 Regions
    public Map<String, RegionInfo> getOnlineRegions() {
        return Collections.unmodifiableMap(onlineRegions);
    }

    public void stop() throws Exception {
        if (cache != null) {
            cache.close();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        onlineRegions.clear();
        lastHeartbeatMap.clear();
        logger.info("RegionWatcher 已停止");
    }
}
