package com.mds.region;

import com.mds.region.handler.MasterHandler;
import com.mds.common.util.MySQLUtil;
import com.mds.region.handler.ClientHandler;
import com.mds.region.handler.ZookeeperHandler;

import org.apache.zookeeper.KeeperException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private ServerSocket serverSocket;
    private final ExecutorService executorService;

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
        this.executorService = Executors.newFixedThreadPool(10);
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

            // 6. 启动统一的请求处理服务
            serverSocket = new ServerSocket(port);
            logger.info("开始监听端口: {}", port);

            // 启动请求处理线程
            startRequestHandler();

            logger.info("RegionServer启动成功: {}", serverId);

        } catch (Exception e) {
            logger.error("RegionServer启动失败", e);
            throw new RuntimeException(e);
        }
    }

    private void startRequestHandler() {
        Thread handlerThread = new Thread(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    executorService.submit(() -> handleRequest(socket));
                } catch (IOException e) {
                    if (!serverSocket.isClosed()) {
                        logger.error("接受连接失败", e);
                    }
                }
            }
        }, "RequestHandler");
        handlerThread.start();
    }

    private void handleRequest(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String requestLine = in.readLine();
            if (requestLine == null)
                return;

            JSONObject request = new JSONObject(requestLine);
            String type = request.getString("type");

            switch (type) {
                case "HEARTBEAT":
                    masterHandler.handleHeartbeat(request, out);
                    break;
                case "CLIENT_REQUEST":
                    clientHandler.handleRequest(request, out);
                    break;
                default:
                    logger.warn("未知的请求类型: {}", type);
                    break;
            }
        } catch (Exception e) {
            logger.error("处理请求失败", e);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                logger.error("关闭Socket失败", e);
            }
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

            // 5. 关闭ServerSocket
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
                logger.info("ServerSocket 关闭");
            }

            // 6. 关闭线程池
            executorService.shutdown();
            logger.info("线程池关闭");

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
            System.out.println("连接已满");
        } else if (currentConnections > 5) {
            connectionStatus = "Busy";
            System.out.println("连接繁忙");
        } else if (currentConnections < 3) {
            connectionStatus = "Idle";
            System.out.println("连接空闲");
        }
        // 更新ZooKeeper中的RegionServer数据
        try {
            if (!zkHandler.isInitialized()) {
                zkHandler.init();
            }
            JSONObject serverData = new JSONObject();
            serverData.put("host", host);
            serverData.put("port", port);
            serverData.put("clientPort", clientPort);
            serverData.put("replicaKey", replicaKey);
            serverData.put("connections", currentConnections);
            serverData.put("status", connectionStatus);
            serverData.put("regions", new JSONObject(regions.keySet()));

            zkHandler.updateRegionServerData(serverId, serverData.toString());
            logger.info("更新RegionServer状态成功: {}", serverData.toString());
        } catch (Exception e) {
            logger.error("更新RegionServer状态失败", e);
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

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        try {
            // 1. 获取服务器配置
            System.out.println("=== RegionServer 配置 ===");
            System.out.print("请输入服务器地址 (默认 localhost): ");
            String host = scanner.nextLine().trim();
            if (host.isEmpty()) {
                host = "localhost";
            }
            System.out.print("请输入服务端口 (默认 8100): ");
            String portStr = scanner.nextLine().trim();
            int port = portStr.isEmpty() ? 8100 : Integer.parseInt(portStr);

            System.out.print("请输入副本标识 (默认 1): ");
            String replicaKey = scanner.nextLine().trim();
            if (replicaKey.isEmpty()) {
                replicaKey = "1";
            }

            // 2. 创建并启动 RegionServer
            System.out.println("\n正在启动 RegionServer...");
            RegionServer server = new RegionServer(host, port, replicaKey);
            server.start();

            // 3. 打印服务器信息
            System.out.println("\n=== RegionServer 信息 ===");
            System.out.println("地址: " + host);
            System.out.println("服务端口: " + port);
            System.out.println("副本标识: " + replicaKey);
            System.out.println("服务器ID: " + server.getServerId());

            // 4. 打印分片信息
            server.printShardingInfo();

            // 5. 等待命令
            System.out.println("\n=== 服务器运行中 (输入 'quit' 退出) ===");
            while (true) {
                String command = scanner.nextLine().trim().toLowerCase();
                if ("quit".equals(command)) {
                    System.out.println("正在停止服务器...");
                    server.stop();
                    break;
                } else if ("status".equals(command)) {
                    System.out.println("当前状态: " + server.getConnectionStatus());
                    System.out.println("当前连接数: " + server.currentConnections);
                    server.printShardingInfo();
                } else if ("help".equals(command)) {
                    System.out.println("可用命令:");
                    System.out.println("  status - 显示服务器状态");
                    System.out.println("  quit   - 退出服务器");
                    System.out.println("  help   - 显示帮助信息");
                }
            }

        } catch (Exception e) {
            System.err.println("服务器运行错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }

    // 用于测试的辅助方法
    // 增强打印方法
    public void printShardingInfo() {
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
