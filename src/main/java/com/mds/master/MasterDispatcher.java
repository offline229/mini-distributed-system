package com.mds.master;

import com.mds.common.RegionServerInfo;
import com.mds.master.self.MetaManager;
import com.mds.common.RegionServerInfo.HostPortStatus;

import java.util.*;

public class MasterDispatcher {
    // 定义相应类型常量
    public static final String RESPONSE_TYPE_DDL_RESULT = "DDL_RESULT";
    public static final String RESPONSE_TYPE_DML_REDIRECT = "DML_REDIRECT";
    public static final String RESPONSE_TYPE_ERROR = "ERROR";

    private final RegionWatcher regionWatcher;
    private final MetaManager metaManager;
    private volatile boolean isRunning = false;     // 是否正在运行

    // Constructor accepting RegionWatcher
    public MasterDispatcher(RegionWatcher regionWatcher) {
        this.regionWatcher = regionWatcher;
        this.metaManager = new MetaManager(regionWatcher); 
    }
    
    public MasterDispatcher(MetaManager metaManager, RegionWatcher regionWatcher) {
        this.metaManager = metaManager;
        this.regionWatcher = regionWatcher;
    }

    // 启动 MasterDispatcher
    public void start() {
        isRunning = true;
        System.out.println("MasterDispatcher started");
    }

    // 停止 MasterDispatcher
    public void stop() {
        isRunning = false;
        System.out.println("MasterDispatcher stopped");
    }

    // 分发 SQL 请求
    public Map<String, Object> dispatch(String sql) {
        Map<String, Object> response = new HashMap<>();

        if (!isRunning) {
            response.put("type", RESPONSE_TYPE_ERROR);
            response.put("message", "Dispatcher is not running");
            return response;
        }

        try {
            // 判断 SQL 类型
            boolean isDML = isDML(sql);

            if (isDML) {    // DML 请求
                return handleDMLRequest(sql);
            } else {        // DDL 请求
                return handleDDLRequest(sql);
            }
        } catch (Exception e) {
            response.put("type", RESPONSE_TYPE_ERROR);
            response.put("message", "Failed to process SQL: " + e.getMessage());
            return response;
        }
    }

    // 处理 DML 请求
    private Map<String, Object> handleDMLRequest(String sql) {
        Map<String, Object> response = new HashMap<>();
        try {
            // 获取所有在线的 RegionServer里负载最低的节点
            RegionServerInfo targetRegion = findOptimalRegionServer();
            
            if (targetRegion == null) {
                response.put("type", RESPONSE_TYPE_ERROR);
                response.put("message", "No available RegionServer");
                return response;
            }

            // 选择负载最低的副本
            HostPortStatus optimalHost = findOptimalHost(targetRegion);
            
            // 打印选中的 host 和 port
            System.out.println("[MasterDispatcher] DML 请求选中的 RegionServer: regionId=" + targetRegion.getRegionserverID() +
                           ", host=" + optimalHost.getHost() + ", port=" + optimalHost.getPort());

            response.put("type", RESPONSE_TYPE_DML_REDIRECT);
            response.put("regionId", targetRegion.getRegionserverID());
            // response.put("replicaKey", targetRegion.getReplicaKey());
            response.put("host", optimalHost.getHost());
            response.put("port", optimalHost.getPort());
            
        } catch (Exception e) {
            response.put("type", RESPONSE_TYPE_ERROR);
            response.put("message", "DML dispatch failed: " + e.getMessage());
        }
        return response;
    }

    private Map<String, Object> handleDDLRequest(String sql) {
    Map<String, Object> response = new HashMap<>();
    try {
        // 获取所有在线的 RegionServer
        List<RegionServerInfo> allRegions = new ArrayList<>(regionWatcher.getOnlineRegions().values());
        System.out.println("[MasterDispatcher] 当前在线 RegionServer 数量: " + allRegions.size());

        // 如果没有可用的 RegionServer，返回错误
        if (allRegions.isEmpty()) {
            response.put("type", RESPONSE_TYPE_ERROR);
            response.put("message", "No available RegionServer");
            return response;
        }

        // 找到负载最小的 RegionServer
        RegionServerInfo optimalRegion = allRegions.stream()
            .min(Comparator.comparingInt(region -> 
                region.getHostsPortsStatusList().stream()
                    .mapToInt(HostPortStatus::getConnections)
                    .min()
                    .orElse(Integer.MAX_VALUE)))
            .orElse(null);

        if (optimalRegion == null) {
            response.put("type", RESPONSE_TYPE_ERROR);
            response.put("message", "No available RegionServer");
            return response;
        }

        // 找到负载最小的副本
        HostPortStatus optimalHost = findOptimalHost(optimalRegion);

        // 打印选中的 host 和 port
        System.out.println("[MasterDispatcher] DDL 请求选中的 RegionServer: regionId=" + optimalRegion.getRegionserverID() +
                           ", host=" + optimalHost.getHost() + ", port=" + optimalHost.getPort());

                           
        response.put("type", RESPONSE_TYPE_DDL_RESULT);
        response.put("regionId", optimalRegion.getRegionserverID());
        response.put("host", optimalHost.getHost());
        response.put("port", optimalHost.getPort());
    } catch (Exception e) {
        response.put("type", RESPONSE_TYPE_ERROR);
        response.put("message", "DDL dispatch failed: " + e.getMessage());
    }
    return response;
}
    
    private RegionServerInfo findOptimalRegionServer() {
        Map<String, RegionServerInfo> onlineRegions = regionWatcher.getOnlineRegions();
        if (onlineRegions.isEmpty()) return null;

        return onlineRegions.values().stream()
            .min(Comparator.comparingInt(region -> 
                region.getHostsPortsStatusList().stream()
                    .mapToInt(HostPortStatus::getConnections)
                    .min()
                    .orElse(Integer.MAX_VALUE)))
            .orElse(null);
    }

    private HostPortStatus findOptimalHost(RegionServerInfo region) {
        return region.getHostsPortsStatusList().stream()
            .min(Comparator.comparingInt(HostPortStatus::getConnections))
            .orElse(region.getHostsPortsStatusList().get(0));
    }

    private boolean isDML(String sql) {
        String trimmedSql = sql.trim().toUpperCase();
        return trimmedSql.startsWith("SELECT") || 
               trimmedSql.startsWith("INSERT") ||
               trimmedSql.startsWith("UPDATE") || 
               trimmedSql.startsWith("DELETE");
    }
}