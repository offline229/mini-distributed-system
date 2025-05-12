package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionInfo;
import com.mds.master.self.MetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MasterDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(MasterDispatcher.class);

    private final MetaManager metaManager;
    private final RegionWatcher regionWatcher;
    // 在客户端直连模式下，DML 路由不再需要轮询计数器，因为 Master 返回的是所有相关 Region 地址
    // private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    private final ObjectMapper objectMapper = new ObjectMapper(); // 用于 JSON 序列化/反序列化

    // 用于从简单的 DML 中提取表名和可选的 ID 的正则表达式
    private static final Pattern DML_PATTERN = Pattern.compile(
            "^(SELECT|INSERT INTO|UPDATE|DELETE FROM)\\s+([\\w_]+)(?:.*WHERE\\s+.*?ID\\s*=\\s*(\\d+))?",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    // 定义返回给 MasterServer 的响应类型
    public static final String RESPONSE_TYPE_DDL_RESULT = "DDL_RESULT";
    public static final String RESPONSE_TYPE_DML_REDIRECT = "DML_REDIRECT";
    public static final String RESPONSE_TYPE_ERROR = "ERROR";


    public MasterDispatcher(MetaManager metaManager, RegionWatcher regionWatcher) {
        this.metaManager = metaManager;
        this.regionWatcher = regionWatcher;
    }

    // 主入口：处理传来的 SQL 请求
    // 返回 Map<String, Object> 包含响应类型和具体数据
    public Map<String, Object> dispatch(String sql) {
        sql = sql.trim();
        Map<String, Object> response = new HashMap<>();
        try {
            String lowerSql = sql.toLowerCase();
            if (lowerSql.startsWith("create") || lowerSql.startsWith("drop") || lowerSql.startsWith("alter")) {
                // 处理 DDL，返回执行结果
                String ddlResult = handleDDL(sql);
                response.put("type", RESPONSE_TYPE_DDL_RESULT);
                response.put("result", ddlResult); // DDL 执行的汇总结果
            } else if (lowerSql.startsWith("select") || lowerSql.startsWith("insert") ||
                    lowerSql.startsWith("update") || lowerSql.startsWith("delete")) {
                // 处理 DML，返回 RegionServer 地址信息供客户端直连
                List<RegionInfo> targetRegions = handleDML(sql);
                response.put("type", RESPONSE_TYPE_DML_REDIRECT);
                response.put("regions", targetRegions); // 目标 RegionInfo 列表
            } else {
                logger.warn("不支持的 SQL 类型: {}", sql);
                response.put("type", RESPONSE_TYPE_ERROR);
                response.put("message", "不支持的 SQL 类型: " + sql);
            }
        } catch (Exception e) {
            logger.error("处理 SQL 发生错误", e);
            response.put("type", RESPONSE_TYPE_ERROR);
            response.put("message", "处理 SQL 异常: " + e.getMessage());
        }
        return response;
    }

    // DDL 处理逻辑
    // Master 处理 DDL 仍然需要广播给 RegionServer
    private String handleDDL(String sql) {
        logger.info("接收到 DDL: {}", sql);
        StringBuilder result = new StringBuilder();

        try {
            // 1. 调用 MetaManager 更新本地元数据 (假设 MetaManager 负责解析和更新)
            // 在客户端直连模式下，Master 的元数据更新仍然很重要
            boolean success = metaManager.updateMetadata(sql); // 假设 updateMetadata 处理 DDL 解析
            if (!success) {
                String errMsg = "DDL 执行失败：Master 元数据更新失败";
                result.append(errMsg);
                logger.error(errMsg + " for SQL: {}", sql);
                // 即使 Master 元数据更新失败，可能还需要尝试广播给 RegionServer，取决于一致性模型
                // 这里简单处理为失败则不再广播
                return result.toString();
            }
            result.append("Master 元数据更新成功. ");

            // 2. 广播给所有在线 Region Server
            Collection<RegionInfo> regions = regionWatcher.getOnlineRegions().values();
            result.append("开始广播 DDL 到 ").append(regions.size()).append(" 个 Region Server.");

            if (regions.isEmpty()) {
                result.append("\n  - 当前没有在线的 Region Server，DDL 未广播。");
                logger.warn("执行 DDL 时没有在线的 Region Server: {}", sql);
                return result.toString();
            }

            for (RegionInfo region : regions) {
                try {
                    // 发送 DDL 到每个 Region Server 并获取响应
                    // Region Server 执行 DDL 并返回结果
                    String regionResponse = sendDDLToRegionServer(region, sql);
                    result.append("\n  - Region Server [").append(region.getRegionId()).append("]: ").append(regionResponse);
                    logger.info("DDL 广播到 Region Server [{}]: {}", region.getRegionId(), regionResponse);
                } catch (Exception e) {
                    result.append("\n  - Region Server [").append(region.getRegionId()).append("] 广播失败: ").append(e.getMessage());
                    logger.error("广播 DDL 到 Region Server [{}] 失败", region.getRegionId(), e);
                }
            }

        } catch (Exception e) {
            logger.error("处理 DDL 发生错误", e);
            result.append("\n[MasterDispatcher] DDL 处理异常: ").append(e.getMessage());
        }

        return result.toString();
    }

    // DML 处理逻辑
    // 返回目标 RegionInfo 列表供客户端直连
    private List<RegionInfo> handleDML(String sql) {
        logger.info("接收到 DML: {}", sql);
        List<RegionInfo> targetRegions = new ArrayList<>();

        try {
            SQLInfo sqlInfo = parseDmlSql(sql);

            // 核心路由逻辑：根据 SQL 信息查找目标 RegionInfo
            if (sqlInfo.getShardKey() != null && sqlInfo.getTableName() != null) {
                // 这是一个简化的例子，实际中需要根据 tableName 和 shardKey 查询 MetaManager
                // MetaManager 应该包含表的分片信息 (例如：哪个表，按哪个列分片，分片范围/哈希值到 Region ID 的映射)
                // 根据分片信息，找到对应的 Region ID
                // 然后根据 Region ID 从 RegionWatcher 找到对应的 RegionInfo (包含 host, port)

                // 示例路由逻辑 (需要替换为真实的 MetaManager 查找)
                // 假设有一个方法在 MetaManager 中：List<String> getRegionIdsForShard(String tableName, String shardKey)
                // 假设有一个方法在 RegionWatcher 中：RegionInfo getRegionById(String regionId)

                // 简单的模拟：假设所有数据都在第一个在线 Region Server 上 (仅为演示)
                // 或者根据 ID 进行哈希/范围计算，然后查找对应的 Region ID
                // List<String> regionIds = metaManager.getRegionIdsForShard(sqlInfo.getTableName(), sqlInfo.getShardKey());

                // 暂时使用轮询作为 fallback，或者作为单 Region 场景的模拟
                // 在客户端直连模式下，即使是单点，也需要返回其地址让客户端直连
                RegionInfo chosenRegion = chooseRegionForDML(); // 仍然需要一个选择策略

                if (chosenRegion != null) {
                    targetRegions.add(chosenRegion);
                    logger.info("根据 DML 路由到 Region Server: {}", chosenRegion.getRegionId());
                    // 在实际分片中，可能需要添加多个 RegionInfo 到 targetRegions
                    // 例如，如果查询跨越多个分片 (如范围查询，或者没有 WHERE 子句的全表扫描)
                    // List<String> allRegionIdsForTable = metaManager.getAllRegionIdsForTable(sqlInfo.getTableName());
                    // for (String regionId : allRegionIdsForTable) {
                    //     RegionInfo region = regionWatcher.getRegionById(regionId);
                    //     if (region != null) {
                    //         targetRegions.add(region);
                    //     }
                    // }
                } else {
                    logger.warn("未能找到负责处理 DML 的 Region Server: {}", sql);
                }


            } else if (sqlInfo.getTableName() != null) {
                // 对于没有 sharding key 的 DML (如 SELECT * FROM table)
                // 可能需要获取所有包含该表分片的 Region Server 地址
                logger.info("DML 不含 sharding key，尝试查找所有相关 Region Server。SQL: {}", sql);
                // 示例：获取所有包含该表分片的 Region Server (需要 MetaManager 支持)
                // List<String> allRegionIdsForTable = metaManager.getAllRegionIdsForTable(sqlInfo.getTableName());
                // for (String regionId : allRegionIdsForTable) {
                //     RegionInfo region = regionWatcher.getRegionById(regionId);
                //     if (region != null) {
                //         targetRegions.add(region);
                //     }
                // }
                // 如果 MetaManager 无法提供详细分片信息，暂时返回所有在线 Region Server 地址 (不准确但可演示)
                logger.warn("MetaManager 缺乏详细分片信息，返回所有在线 Region Server 地址 (可能不准确)。");
                targetRegions.addAll(regionWatcher.getOnlineRegions().values());
            }
            else {
                logger.warn("无法解析 DML 语句以提取表名或 sharding key，无法路由: {}", sql);
            }


        } catch (Exception e) {
            logger.error("处理 DML 发生错误", e);
            // 在客户端直连模式下，DML 错误应在 Region Server 端发生并报告给客户端
            // Master 这里的错误通常是路由错误（如找不到表或 Region Server）
            // 返回空列表或包含错误信息的列表（如果客户端能处理）
            // 这里返回空列表，由 MasterServer 构建 ERROR 响应
        }
        return targetRegions; // 返回找到的目标 RegionInfo 列表
    }

    // 简单的策略选择一个 Region，例如用于没有明确分片键的 DML 或作为演示
    // 在真正的分片系统中，这里逻辑会复杂得多
    private RegionInfo chooseRegionForDML() {
        List<RegionInfo> regions = new ArrayList<>(regionWatcher.getOnlineRegions().values());
        if (regions.isEmpty()) return null;
        // 可以根据负载或其他策略选择
        // 这里简单返回第一个在线的 RegionInfo
        return regions.get(0); // 或者使用轮询等更高级的策略
    }


    // 通过 socket 将 DDL 请求发送到特定的 Region Server
    // 这个方法仅用于 Master -> RegionServer 的 DDL 或其他管理通信
    private String sendDDLToRegionServer(RegionInfo region, String ddlSql) throws Exception {
        logger.debug("发送 DDL 到 Region Server [{}] at {}:{}", region.getRegionId(), region.getHost(), region.getPort());
        try (Socket socket = new Socket(region.getHost(), region.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 构建发送给 RegionServer 的请求
            // 假设 RegionServer 监听一个端口，并能够区分来自 Master 的 DDL 请求
            Map<String, Object> request = new HashMap<>();
            request.put("type", "EXECUTE_DDL"); // 定义一个给 RegionServer 的 DDL 执行类型
            request.put("sql", ddlSql);
            String jsonRequest = objectMapper.writeValueAsString(request);
            out.println(jsonRequest);
            logger.debug("发送 JSON DDL 请求到 Region Server [{}]: {}", region.getRegionId(), jsonRequest);

            // 读取 Region Server 对 DDL 的响应
            String jsonResponse = in.readLine();
            if (jsonResponse == null) {
                logger.error("Region Server [{}] 执行 DDL 未返回响应", region.getRegionId());
                throw new IOException("Region Server [" + region.getRegionId() + "] 执行 DDL 未返回响应");
            }
            logger.debug("收到 Region Server [{}] DDL 响应: {}", region.getRegionId(), jsonResponse);

            // 解析 JSON 响应
            Map<String, Object> response = objectMapper.readValue(jsonResponse, Map.class);
            if ("ok".equalsIgnoreCase((String) response.get("status"))) {
                return "成功: " + response.get("message"); // 假设 Region Server 返回成功消息
            } else {
                return "失败: " + response.get("message"); // 假设 Region Server 返回错误消息
            }

        } catch (IOException e) {
            logger.error("发送 DDL 到 Region Server [{}] 失败: {}:{}", region.getRegionId(), region.getHost(), region.getPort(), e);
            throw e; // 重新抛出异常，由调用方处理
        }
    }


    // 基本的 DML SQL 解析，提取表名和可选的 ID
    private SQLInfo parseDmlSql(String sql) {
        SQLInfo info = new SQLInfo();
        Matcher matcher = DML_PATTERN.matcher(sql.trim());

        if (matcher.find()) {
            info.operation = matcher.group(1).toUpperCase();
            info.tableName = matcher.group(2).toLowerCase();
            // Group 3 是从 WHERE 子句中提取的 ID (如果存在)
            info.shardKey = matcher.group(3);
            logger.debug("解析 DML 成功: {}", info);
        } else {
            logger.warn("无法解析 DML 语句以提取表名或 sharding key: {}", sql);
            // 对于无法解析的 DML，tableName 和 shardKey 将保持 null
        }

        return info;
    }

    // 辅助类，用于保存解析后的 SQL 信息
    private static class SQLInfo {
        String operation; // SELECT, INSERT INTO, UPDATE, DELETE FROM
        String tableName;
        String shardKey; // 从 WHERE 子句中提取的 ID

        public String getOperation() {
            return operation;
        }

        public String getTableName() {
            return tableName;
        }

        public String getShardKey() {
            return shardKey;
        }

        @Override
        public String toString() {
            return "SQLInfo{" +
                    "operation='" + operation + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", shardKey='" + shardKey + '\'' +
                    '}';
        }
    }

    // Start method can remain or be removed as needed
    public void start() {
        logger.info("MasterDispatcher 启动完成，等待 SQL 请求调度...");
    }
}