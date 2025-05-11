package com.mds.master;

import com.mds.master.self.MetaManager;
import com.mds.region.Region;

import java.net.HttpURLConnection;
import java.net.URL;
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
    public String dispatch(String sql) throws Exception {
        sql = sql.trim().toLowerCase();

        if (isDDL(sql)) {
            return handleDDL(sql);
        } else if (isDML(sql)) {
            return handleDML(sql);
        } else {
            return "Unsupported SQL type.";
        }
    }

    // DDL 处理逻辑
    private String handleDDL(String sql) throws Exception {
        System.out.println("[Master] 接收到 DDL 请求: " + sql);

        // 调用 MetaManager 更新本地元数据（示例）
        boolean success = metaManager.updateMetadata(sql);
        if (!success) {
            return "DDL 执行失败：元数据更新失败";
        }

        // 广播 DDL 请求到所有在线 Region
        Map<String, Region> regions = regionWatcher.getOnlineRegions();
        for (Region region : regions.values()) {
            sendSQLToRegion(sql, region);
        }

        return "DDL 执行成功并广播完成";
    }

    // DML 处理逻辑
    private String handleDML(String sql) throws Exception {
        System.out.println("[Master] 接收到 DML 请求: " + sql);

        String tableName = extractTableName(sql);
        if (tableName == null) {
            return "无法识别表名，DML 调度失败";
        }

        // 查找该表所属的 Region
        Region targetRegion = metaManager.getRegionForTable(tableName);
        if (targetRegion == null) {
            return "未找到表 " + tableName + " 的 Region 分布信息";
        }

        // 发送到对应 RegionServer 执行
        String result = sendSQLToRegion(sql, targetRegion);
        return "DML 执行成功，Region 返回: " + result;
    }

    // 模拟发送 SQL 到 Region
    private String sendSQLToRegion(String sql, Region region) throws Exception {
        URL url = new URL("http://" + region.getHost() + ":" + region.getPort() + "/execute?sql=" + sql);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET"); // 假设 RegionServer 提供 REST 接口
        int code = conn.getResponseCode();
        return (code == 200) ? "OK" : "ERROR";
    }

    // 简单判断是否是 DDL
    private boolean isDDL(String sql) {
        return sql.startsWith("create") || sql.startsWith("drop") || sql.startsWith("alter");
    }

    // 简单判断是否是 DML
    private boolean isDML(String sql) {
        return sql.startsWith("select") || sql.startsWith("insert")
                || sql.startsWith("update") || sql.startsWith("delete");
    }

    // 提取 SQL 中的表名（简化实现）
    private String extractTableName(String sql) {
        String[] tokens = sql.split("\\s+");
        for (int i = 0; i < tokens.length - 1; i++) {
            if ((tokens[i].equals("from") || tokens[i].equals("into") || tokens[i].equals("update"))) {
                return tokens[i + 1];
            }
        }
        return null;
    }

    // 可供主节点初始化时调用，进入服务状态
    public void start() {
        System.out.println("MasterDispatcher 启动完成，等待 SQL 请求调度...");
        // 通常此处会开启 RPC 监听（如 Socket 或 Netty）来接收客户端 SQL
    }
}
