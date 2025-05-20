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
        System.out.println("[MasterDispatcher] started");
    }

    // 停止 MasterDispatcher
    public void stop() {
        isRunning = false;
        System.out.println("[MasterDispatcher] stopped");
    }

    // 分发 SQL 请求
    public Map<String, Object> dispatch(String sql) {
        if (!isRunning) {
            return buildErrorResponse("Dispatcher is not running");
        }
        try {
            boolean isDML = isDML(sql);
            return handleSqlRequest(isDML);
        } catch (Exception e) {
            return buildErrorResponse("Failed to process SQL: " + e.getMessage());
        }
    }

    // 统一处理 DML/DDL 请求
    private Map<String, Object> handleSqlRequest(boolean isDML) {
        List<RegionServerInfo> allRegions = new ArrayList<>(regionWatcher.getOnlineRegions().values());
        if (allRegions.isEmpty()) {
            return buildErrorResponse("No available RegionServer");
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
            return buildErrorResponse("No available RegionServer");
        }

        // 找到负载最小的副本
        HostPortStatus optimalHost = findOptimalHost(optimalRegion);

        // 打印选中的 host 和 port
        System.out.println("[MasterDispatcher] " + (isDML ? "DML" : "DDL") +
                " 请求选中的 RegionServer: regionId=" + optimalRegion.getRegionserverID() +
                ", host=" + optimalHost.getHost() + ", port=" + optimalHost.getPort());

        String type = isDML ? RESPONSE_TYPE_DML_REDIRECT : RESPONSE_TYPE_DDL_RESULT;
        return buildRegionResponse(type, optimalRegion, optimalHost);
    }

    // 构建正常响应
    private Map<String, Object> buildRegionResponse(String type, RegionServerInfo region, HostPortStatus host) {
        Map<String, Object> response = new HashMap<>();
        response.put("type", type);
        response.put("regionId", region.getRegionserverID());
        response.put("host", host.getHost());
        response.put("port", host.getPort());
        return response;
    }

    // 构建错误响应
    private Map<String, Object> buildErrorResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("type", RESPONSE_TYPE_ERROR);
        response.put("message", message);
        return response;
    }

    // 找到负载最小的副本
    private HostPortStatus findOptimalHost(RegionServerInfo region) {
        return region.getHostsPortsStatusList().stream()
            .min(Comparator.comparingInt(HostPortStatus::getConnections))
            .orElse(region.getHostsPortsStatusList().get(0));
    }

    // 判断是否为 DML
    private boolean isDML(String sql) {
        String trimmedSql = sql.trim().toUpperCase();
        return trimmedSql.startsWith("SELECT") ||
               trimmedSql.startsWith("INSERT") ||
               trimmedSql.startsWith("UPDATE") ||
               trimmedSql.startsWith("DELETE");
    }
}