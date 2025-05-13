package com.mds.master;

import com.mds.common.RegionServerInfo;
import com.mds.master.self.MetaManager;
import com.mds.common.RegionServerInfo.HostPortStatus;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class MasterDispatcher {
    public static final String RESPONSE_TYPE_DDL_RESULT = "DDL_RESULT";
    public static final String RESPONSE_TYPE_DML_REDIRECT = "DML_REDIRECT";
    public static final String RESPONSE_TYPE_ERROR = "ERROR";

    private final RegionWatcher regionWatcher;
    private final MetaManager metaManager;
    private volatile boolean isRunning = false;

    // Constructor accepting RegionWatcher
    public MasterDispatcher(RegionWatcher regionWatcher) {
        this.regionWatcher = regionWatcher;
        this.metaManager = new MetaManager(regionWatcher); 
    }
    
    public MasterDispatcher(MetaManager metaManager, RegionWatcher regionWatcher) {
        this.metaManager = metaManager;
        this.regionWatcher = regionWatcher;
    }

    public void start() {
        isRunning = true;
        System.out.println("MasterDispatcher started");
    }

    public void stop() {
        isRunning = false;
        System.out.println("MasterDispatcher stopped");
    }

    public Map<String, Object> dispatch(String sql) {
        Map<String, Object> response = new HashMap<>();

        if (!isRunning) {
            response.put("type", RESPONSE_TYPE_ERROR);
            response.put("message", "Dispatcher is not running");
            return response;
        }

        try {
            boolean isDML = isDML(sql);

            if (isDML) {
                return handleDMLRequest(sql);
            } else {
                return handleDDLRequest(sql);
            }
        } catch (Exception e) {
            response.put("type", RESPONSE_TYPE_ERROR);
            response.put("message", "Failed to process SQL: " + e.getMessage());
            return response;
        }
    }

    private Map<String, Object> handleDMLRequest(String sql) {
        Map<String, Object> response = new HashMap<>();
        try {
            RegionServerInfo targetRegion = findOptimalRegionServer();
            
            if (targetRegion == null) {
                response.put("type", RESPONSE_TYPE_ERROR);
                response.put("message", "No available RegionServer");
                return response;
            }

            // 选择负载最低的副本
            HostPortStatus optimalHost = findOptimalHost(targetRegion);
            
            response.put("type", RESPONSE_TYPE_DML_REDIRECT);
            response.put("regionId", targetRegion.getRegionserverID());
            response.put("replicaKey", targetRegion.getReplicaKey());
            response.put("host", optimalHost.getHost());
            response.put("port", optimalHost.getPort());
            
        } catch (Exception e) {
            response.put("type", RESPONSE_TYPE_ERROR);
            response.put("message", "DML dispatch failed: " + e.getMessage());
        }
        return response;
    }

    // private Map<String, Object> handleDDLRequest(String sql) {
    //     Map<String, Object> response = new HashMap<>();
    //     try {
    //         List<RegionServerInfo> allRegions = new ArrayList<>(regionWatcher.getOnlineRegions().values());
    //         Set<String> processedReplicaKeys = new HashSet<>();
    //         List<Map<String, Object>> regionDetails = new ArrayList<>();

    //         for (RegionServerInfo region : allRegions) {
    //             // 对于DDL，每个副本组只选择一个节点
    //             if (processedReplicaKeys.add(region.getReplicaKey())) {
    //                 HostPortStatus optimalHost = findOptimalHost(region);
    //                 Map<String, Object> regionInfo = new HashMap<>();
    //                 regionInfo.put("regionId", region.getRegionserverID());
    //                 regionInfo.put("replicaKey", region.getReplicaKey());
    //                 regionInfo.put("host", optimalHost.getHost());
    //                 regionInfo.put("port", optimalHost.getPort());
    //                 regionDetails.add(regionInfo);
    //             }
    //         }

    //         response.put("type", RESPONSE_TYPE_DDL_RESULT);
    //         response.put("regions", regionDetails);
            
    //     } catch (Exception e) {
    //         response.put("type", RESPONSE_TYPE_ERROR);
    //         response.put("message", "DDL dispatch failed: " + e.getMessage());
    //     }
    //     return response;
    // }

    private Map<String, Object> handleDDLRequest(String sql) {
    Map<String, Object> response = new HashMap<>();
    try {
        // 获取所有在线的 RegionServer
        List<RegionServerInfo> allRegions = new ArrayList<>(regionWatcher.getOnlineRegions().values());
        System.out.println("[MasterDispatcher] 当前在线 RegionServer 数量: " + allRegions.size());

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