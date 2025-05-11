package com.mds.master;

import com.mds.common.RegionInfo;
import com.mds.master.self.MetaManager;
import com.mds.region.Region;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class MasterDispatcher {
    private final MetaManager metaManager;
    private final RegionWatcher regionWatcher;

    public MasterDispatcher(MetaManager metaManager, RegionWatcher regionWatcher) {
        this.metaManager = metaManager;
        this.regionWatcher = regionWatcher;
    }

    // 主入口：处理客户端传来的 SQL 请求
    public String dispatch(String sql) {
        sql = sql.trim().toLowerCase();
        try {
            if (sql.startsWith("create") || sql.startsWith("drop") || sql.startsWith("alter")) {
                return handleDDL(sql);
            } else if (sql.startsWith("select") || sql.startsWith("insert") ||
                    sql.startsWith("update") || sql.startsWith("delete")) {
                return handleDML(sql);
            } else {
                return "[MasterDispatcher] 不支持的 SQL 类型: " + sql;
            }
        } catch (Exception e) {
            return "[MasterDispatcher] 错误：" + e.getMessage();
        }
    }

    // DDL 处理逻辑
    private String handleDDL(String sql) throws Exception {
        System.out.println("[MasterDispatcher] 接收到 DDL：" + sql);

        // 1.调用 MetaManager 更新本地元数据
        boolean success = metaManager.updateMetadata(sql);
        if (!success) {
            return "DDL 执行失败：元数据更新失败";
        }

        // 2. 广播给所有 Region
        Collection<RegionInfo> regions = regionWatcher.getOnlineRegions().values();
        for (RegionInfo region : regions) {
            sendToRegion(region, sql);
        }

        return "[MasterDispatcher] DDL 广播完成，Region 数量：" + regions.size();    }

    // DML 处理逻辑
    private String handleDML(String sql) throws Exception {
        System.out.println("[MasterDispatcher] 接收到 DML：" + sql);

        RegionInfo target = chooseRegion();
        if (target == null) {
            return "[MasterDispatcher] 无可用 Region 节点，拒绝请求。";
        }

        sendToRegion(target, sql);
        return "[MasterDispatcher] 已转发到 Region: " + target.getRegionId();
    }

    //简单轮询选 Region（可替换为最小负载）
    private RegionInfo chooseRegion() {
        List<RegionInfo> regions = new ArrayList<>(regionWatcher.getOnlineRegions().values());
        if (regions.isEmpty()) return null;
        int index = roundRobinCounter.getAndIncrement() % regions.size();
        return regions.get(index);
    }

    // 可供主节点初始化时调用，进入服务状态
    public void start() {
        System.out.println("MasterDispatcher 启动完成，等待 SQL 请求调度...");
        // 通常此处会开启 RPC 监听（如 Socket 或 Netty）来接收客户端 SQL
    }
}
