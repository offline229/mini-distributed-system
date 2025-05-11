package com.mds.region;

import com.mds.region.handler.MasterHandler;
import com.mds.common.util.MySQLUtil;
import com.mds.region.handler.ClientHandler;
import com.mds.region.handler.ZookeeperHandler;

import org.apache.zookeeper.KeeperException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class RegionServer {
    private static final Logger logger = LoggerFactory.getLogger(RegionServer.class);
    private static final int MAX_REGIONS = 10;
    private static final int SHARD_SIZE = 10; // 每个分片1000条数据

    private final String host;
    private final int port;
    private String serverId;
    private final String replicaKey; // 添加副本标识key

    private final ZookeeperHandler zkHandler;
    private final ClientHandler clientHandler;
    private final MasterHandler masterHandler;
    // 本地存储分片信息
    private final Map<String, Region> regions; // regionId -> Region
    private final Map<String, Map<String, String>> tableShards; // tableName -> (shardRange -> regionId)

    // 添加副本相关字段
    private final Map<String, RegionServer> replicas; // replicaKey -> RegionServer
    private int currentConnections; // 当前连接数
    private String connectionStatus = "Idle"; // 默认状态是空闲

    public RegionServer(String host, int port, String replicaKey) {
        this.host = host;
        this.port = port;
        this.replicaKey = replicaKey;
        this.regions = new ConcurrentHashMap<>();
        this.tableShards = new ConcurrentHashMap<>();
        this.replicas = new ConcurrentHashMap<>();
        this.currentConnections = 0;
        this.zkHandler = new ZookeeperHandler();
        this.clientHandler = new ClientHandler(this);
        this.masterHandler = new MasterHandler();
    }

    private int clientPort = -1; // -1 表示没有指定端口

    // 用来设置客户端端口
    public void setClientPort(int port) {
        this.clientPort = port;
    }

    public int getClientPort() {
        return clientPort;
    }

    public void start() {
        try {
            // 1. 初始化ZK连接
            zkHandler.init();
            logger.info("ZooKeeper连接成功");

            // 2. 从Master获取serverId
            masterHandler.init();
            this.serverId = masterHandler.registerRegionServer(host, port, replicaKey);
            logger.info("从Master获取到serverId: {}", serverId);

            // 3. 获取表信息
            Map<String, Long> tableRows = getTableRows();
            logger.info("获取到表信息: {}", tableRows);

            // 4. 生成分片并初始化Regions
            createRegionsWithShards(tableRows);
            logger.info("Region分片初始化完成");

            // 5. 更新ZK节点信息
            updateZkInfo();
            logger.info("ZK节点更新完成");

            // 6. 启动客户端处理器
            clientHandler.start();
            logger.info("RegionServer启动成功: {}", serverId);

        } catch (Exception e) {
            logger.error("RegionServer启动失败", e);
            throw new RuntimeException(e);
        }
    }

    // 添加获取最少连接的RegionServer方法
    public RegionServer getLeastLoadedReplica() {
        RegionServer leastLoaded = this;
        int minConnections = this.currentConnections;

        for (RegionServer replica : replicas.values()) {
            if (replica.currentConnections < minConnections) {
                minConnections = replica.currentConnections;
                leastLoaded = replica;
            }
        }

        return leastLoaded;
    }

    public String getServerId() {
        return serverId;
    }

    public void stop() {
        try {
            // 1. 停止客户端处理器
            if (clientHandler != null) {
                clientHandler.stop(); // 假设 ClientHandler 有一个 stop() 方法来停止处理器
                logger.info("客户端处理器停止");
            }

            // 2. 清理与 ZooKeeper 的连接
            if (zkHandler != null) {
                zkHandler.close(); // 假设 ZookeeperHandler 有一个 close() 方法来关闭连接
                logger.info("ZooKeeper 连接关闭");
            }

            // 3. 清理与 Master 的连接
            if (masterHandler != null) {
                masterHandler.stop(); // 假设 MasterHandler 有一个 unregisterRegionServer 方法来注销 RegionServer
                logger.info("RegionServer 从 Master 注销");
            }

            // 4. 停止所有分片
            for (Region region : regions.values()) {
                region.stop(); // 假设 Region 有一个 stop() 方法来停止其操作
                logger.info("Region {} 停止", region.getRegionId());
            }

            logger.info("RegionServer {} 已停止", serverId);

        } catch (Exception e) {
            logger.error("停止 RegionServer 失败", e);
        }
    }

    // 增加连接数
    public void incrementConnections() {
        currentConnections++;
        updateConnectionStatus();
        logger.info("当前连接数: {}", currentConnections);
    }

    // 减少连接数
    public void decrementConnections() {
        currentConnections--;
        updateConnectionStatus();
        logger.info("当前连接数: {}", currentConnections);
    }

    // 更新连接状态
    private void updateConnectionStatus() {
        if (currentConnections == 10) {
            connectionStatus = "Full";
        } else if (currentConnections > 5) {
            connectionStatus = "Busy";
        } else if (currentConnections < 3) {
            connectionStatus = "Idle";
        }
        // 更新ZooKeeper中的RegionServer数据
        try {
            zkHandler.updateRegionServerData(serverId, buildServerData());
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error updating RegionServer data in ZooKeeper", e);
        }
    }

    // 构建RegionServer数据
    private String buildServerData() {
        return String.format("{\"host\": \"%s\", \"port\": %d, \"status\": \"%s\"}", host, port, connectionStatus);
    }

    // 获取当前连接状态
    public String getConnectionStatus() {
        return connectionStatus;
    }

    // 添加数据同步方法
    public void syncData(String tableName, String operation, String data) {
        logger.info("同步数据到副本: table={}, operation={}, data={}", tableName, operation, data);
        // 在实际应用中，这里应该实现真正的数据同步
        // 这里简单打印一下
        System.out.println("同步数据到副本: " + tableName + ", " + operation + ", " + data);
    }

    private Map<String, Long> getTableRows() throws SQLException {
        Map<String, Long> tableRows = new HashMap<>();
        String sql = "SHOW TABLES";

        try (Connection conn = MySQLUtil.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String tableName = rs.getString(1);
                // 获取每个表的记录数
                String countSql = "SELECT COUNT(*) FROM " + tableName;
                try (PreparedStatement countStmt = conn.prepareStatement(countSql);
                        ResultSet countRs = countStmt.executeQuery()) {
                    if (countRs.next()) {
                        long rowCount = countRs.getLong(1);
                        tableRows.put(tableName, rowCount);
                        logger.info("表 {} 的记录数: {}", tableName, rowCount);
                    }
                }
            }
        }

        if (tableRows.isEmpty()) {
            logger.warn("数据库中没有找到任何表");
        }

        return tableRows;
    }

    private void createRegionsWithShards(Map<String, Long> tableRows) {
        for (Map.Entry<String, Long> entry : tableRows.entrySet()) {
            String tableName = entry.getKey();
            long totalRows = entry.getValue();
            int shardCount = (int) Math.min(MAX_REGIONS,
                    Math.ceil((double) totalRows / SHARD_SIZE));

            Map<String, String> shards = new HashMap<>();
            for (int i = 0; i < shardCount; i++) {
                // 创建Region
                String regionId = String.format("%s_region_%d", serverId, i);
                Region region = new Region(this, regionId);
                regions.put(regionId, region);

                // 修改分片范围计算
                int start = i * SHARD_SIZE;
                int end = (i == shardCount - 1) ? start + SHARD_SIZE - 1 : start + SHARD_SIZE - 1;
                String shardRange = String.format("%d-%d", start, end);

                shards.put(shardRange, regionId);
                region.start();
            }
            tableShards.put(tableName, shards);
        }
    }

    private void updateZkInfo() throws Exception {
        // 1. 创建分片信息
        JSONObject zkData = new JSONObject();

        // 2. 添加表分片映射
        zkData.put("tables", new JSONObject(tableShards));

        // 3. 添加regions信息
        JSONObject regionsInfo = new JSONObject();
        for (Map.Entry<String, Region> entry : regions.entrySet()) {
            String regionId = entry.getKey();
            JSONObject regionInfo = new JSONObject();
            regionInfo.put("id", regionId);

            // 获取该region负责的所有分片
            Map<String, String> regionShards = new HashMap<>();
            for (Map.Entry<String, Map<String, String>> tableEntry : tableShards.entrySet()) {
                String tableName = tableEntry.getKey();
                for (Map.Entry<String, String> shard : tableEntry.getValue().entrySet()) {
                    if (shard.getValue().equals(regionId)) {
                        regionShards.put(tableName, shard.getKey());
                    }
                }
            }
            regionInfo.put("shards", regionShards);
            regionsInfo.put(regionId, regionInfo);
        }
        zkData.put("regions", regionsInfo);

        // 更新到ZK
        zkHandler.updateRegionServerData(serverId, zkData.toString());
    }

    public Object handleRequest(String sql, Object[] params) throws Exception {
        // 1. SQL语句检查和解析
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL语句不能为空");
        }

        SQLInfo sqlInfo = parseSql(sql.trim().toUpperCase());
        logger.debug("解析SQL结果: {}", sqlInfo);

        // 2. 查找合适的Region
        Region targetRegion = findTargetRegion(sqlInfo);
        if (targetRegion == null) {
            throw new IllegalStateException("未找到合适的Region处理该请求");
        }

        // 3. 转发请求并返回结果
        return targetRegion.execute(sql, params);
    }

    private SQLInfo parseSql(String sql) {
        SQLInfo info = new SQLInfo();
        sql = sql.trim().toUpperCase();

        // 1. 处理 CREATE TABLE
        if (sql.startsWith("CREATE TABLE")) {
            info.operation = "CREATE";
            Pattern pattern = Pattern.compile("CREATE\\s+TABLE\\s+([\\w_]+)");
            Matcher matcher = pattern.matcher(sql);
            if (matcher.find()) {
                info.tableName = matcher.group(1).toLowerCase();
                // CREATE TABLE 不需要分片键，使用最空闲的 region
                return info;
            }
        }

        // 2. 提取表名
        Pattern tablePattern;
        if (sql.contains(" FROM ")) {
            tablePattern = Pattern.compile("FROM\\s+([\\w_]+)");
        } else if (sql.contains("INSERT INTO")) {
            tablePattern = Pattern.compile("INTO\\s+([\\w_]+)");
        } else if (sql.startsWith("UPDATE")) {
            tablePattern = Pattern.compile("UPDATE\\s+([\\w_]+)");
        } else {
            throw new IllegalArgumentException("不支持的SQL操作: " + sql);
        }

        Matcher tableMatcher = tablePattern.matcher(sql);
        if (tableMatcher.find()) {
            info.tableName = tableMatcher.group(1).toLowerCase();
        }

        // 3. 提取 ID（如果存在）
        if (sql.contains("WHERE")) {
            Pattern idPattern = Pattern.compile("WHERE.*ID\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
            Matcher idMatcher = idPattern.matcher(sql);
            if (idMatcher.find()) {
                info.shardKey = idMatcher.group(1);
            }
        }

        return info;
    }

    // 修改 findTargetRegion 方法
    private Region findTargetRegion(SQLInfo sqlInfo) {
        // 处理 CREATE TABLE
        if ("CREATE".equals(sqlInfo.operation)) {
            Region emptyRegion = findEmptyRegion();
            logger.info("为新表 {} 选择 Region: {}", sqlInfo.tableName, emptyRegion.getRegionId());
            return emptyRegion;
        }

        // 检查表是否存在
        Map<String, String> tableShardInfo = tableShards.get(sqlInfo.tableName);
        if (tableShardInfo == null) {
            throw new IllegalArgumentException("表不存在: " + sqlInfo.tableName);
        }

        // 有 ID 的情况
        if (sqlInfo.shardKey != null) {
            int key = Integer.parseInt(sqlInfo.shardKey);
            for (Map.Entry<String, String> entry : tableShardInfo.entrySet()) {
                String[] range = entry.getKey().split("-");
                int start = Integer.parseInt(range[0]);
                int end = Integer.parseInt(range[1]);

                if (key >= start && key <= end) {
                    Region targetRegion = regions.get(entry.getValue());
                    logger.info("根据ID {} 选择 Region: {}", key, targetRegion.getRegionId());
                    return targetRegion;
                }
            }
        }

        // 无 ID 的情况，选择第一个包含该表的 region
        String firstRegionId = tableShardInfo.values().iterator().next();
        Region targetRegion = regions.get(firstRegionId);
        logger.info("无 ID，为表 {} 选择默认 Region: {}", sqlInfo.tableName, targetRegion.getRegionId());
        return targetRegion;
    }

    // 查找最空闲的 Region
    private Region findEmptyRegion() {
        return regions.values().stream()
                .min((r1, r2) -> {
                    long count1 = getRegionTableCount(r1);
                    long count2 = getRegionTableCount(r2);
                    return Long.compare(count1, count2);
                })
                .orElseThrow(() -> new IllegalStateException("没有可用的Region"));
    }

    // 获取 Region 中的表数量
    private long getRegionTableCount(Region region) {
        return tableShards.values().stream()
                .flatMap(shards -> shards.values().stream())
                .filter(regionId -> regionId.equals(region.getRegionId()))
                .count();
    }

    private static class SQLInfo {
        String operation; // 操作类型
        String tableName; // 表名
        String shardKey; // 分片键值

        @Override
        public String toString() {
            return String.format("SQLInfo{operation='%s', table='%s', shardKey='%s'}",
                    operation, tableName, shardKey);
        }
    }

    // 测试用主方法
    public static void main(String[] args) {
        RegionServer server = new RegionServer("localhost", 8000, "1");
        server.start();

        // 打印分片信息
        server.printShardingInfo();

        try {
            // 测试SQL请求
            System.out.println("\n=== SQL请求测试 ===");

            // 1. 测试带分片键的查询
            String sql1 = "SELECT * FROM users WHERE id = 5";
            System.out.println("\n执行SQL: " + sql1);
            Object result1 = server.handleRequest(sql1, null);
            System.out.println("查询结果: " + result1);

            // 2. 测试不带分片键的查询
            String sql2 = "SELECT * FROM users";
            System.out.println("\n执行SQL: " + sql2);
            Object result2 = server.handleRequest(sql2, null);
            System.out.println("查询结果: " + result2);

            // 3. 测试插入操作
            String sql3 = "INSERT INTO users(id, name, age) VALUES(?, ?, ?)";
            Object[] params = { 15, "test_user", 25 }; // 假设为用户提供了一个年龄值，例如 25
            System.out.println("\n执行SQL: " + sql3 + ", 参数: [15, 'test_user', 25]");
            Object result3 = server.handleRequest(sql3, params);
            System.out.println("插入结果: " + result3);

            // 4. 测试更新操作
            String sql4 = "UPDATE users SET name = ? WHERE id = ?";
            Object[] updateParams = { "updated_user", 15 };
            System.out.println("\n执行SQL: " + sql4 + ", 参数: ['updated_user', 15]");
            Object result4 = server.handleRequest(sql4, updateParams);
            System.out.println("更新结果: " + result4);

        } catch (Exception e) {
            System.err.println("测试执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 用于测试的辅助方法
    // 增强打印方法
    private void printShardingInfo() {
        System.out.println("\n=== 表分片信息 ===");
        tableShards.forEach((table, shards) -> {
            System.out.println("\nTable: " + table);
            shards.forEach((range, regionId) -> System.out.printf("  分片范围: %s -> Region: %s\n", range, regionId));
        });

        System.out.println("\n=== Region分片信息 ===");
        regions.forEach((regionId, region) -> {
            System.out.println("\nRegion: " + regionId);
            System.out.println("包含的分片:");
            tableShards.forEach((tableName, shards) -> {
                shards.forEach((range, rid) -> {
                    if (rid.equals(regionId)) {
                        System.out.printf("  表: %s, 分片范围: %s\n", tableName, range);
                    }
                });
            });
        });
    }

    // 处理数据变更
    public void onDataChanged(String operation, String tableName, Object data, String message) {
        try {
            logger.info("收到数据变更通知: 操作={}, 表={}, 消息={}", operation, tableName, message);

            // 1. 更新本地分片信息
            updateLocalShardingInfo(operation, tableName, data);

            // 2. 更新ZK节点信息
            updateZkInfo();

            logger.info("数据变更处理完成: 表={}", tableName);
        } catch (Exception e) {
            logger.error("处理数据变更失败", e);
            throw new RuntimeException("处理数据变更失败", e);
        }
    }

    private void updateLocalShardingInfo(String operation, String tableName, Object data) {
        Map<String, String> tableShardInfo = tableShards.get(tableName);
        if (tableShardInfo == null) {
            // 如果是新建表操作，创建新的分片信息
            if ("CREATE".equals(operation)) {
                tableShardInfo = new HashMap<>();
                tableShards.put(tableName, tableShardInfo);
                logger.info("为新表 {} 创建分片信息", tableName);
            } else {
                logger.warn("表 {} 不存在分片信息", tableName);
                return;
            }
        }

        // 根据操作类型更新分片信息
        switch (operation) {
            case "CREATE":
                // 新建表时，选择一个空闲的Region
                Region emptyRegion = findEmptyRegion();
                String shardRange = "0-0"; // 初始分片范围
                tableShardInfo.put(shardRange, emptyRegion.getRegionId());
                logger.info("表 {} 初始分片范围: {} -> Region: {}",
                        tableName, shardRange, emptyRegion.getRegionId());
                break;

            case "INSERT":
                // 插入操作可能需要扩展分片范围
                if (data instanceof Integer) {
                    int affectedRows = (Integer) data;
                    if (affectedRows > 0) {
                        // 这里可以添加分片范围扩展的逻辑
                        logger.info("表 {} 插入 {} 行数据", tableName, affectedRows);
                    }
                }
                break;

            case "DELETE":
                // 删除操作可能需要收缩分片范围
                if (data instanceof Integer) {
                    int affectedRows = (Integer) data;
                    if (affectedRows > 0) {
                        // 这里可以添加分片范围收缩的逻辑
                        logger.info("表 {} 删除 {} 行数据", tableName, affectedRows);
                    }
                }
                break;
        }
    }
}
