package com.mds.region.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import com.mds.common.service.RegionService;
import com.mds.common.util.ZookeeperUtil;
import com.mds.common.util.MySQLUtil;
import com.mds.common.config.SystemConfig;
import com.mds.region.communication.MasterClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RegionServiceImpl implements RegionService {
    private static final Logger logger = LoggerFactory.getLogger(RegionServiceImpl.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final ZookeeperUtil zkUtil;
    private final String regionId;
    private final String regionPath;
    private final MasterClient masterClient;
    private final ScheduledExecutorService scheduler;
    private static final int HEARTBEAT_INTERVAL = 5; // 心跳间隔（秒）
    private static final int STATUS_REPORT_INTERVAL = 30; // 状态报告间隔（秒）

    public RegionServiceImpl(String regionId) throws Exception {
        this.regionId = regionId;
        this.zkUtil = new ZookeeperUtil();
        this.regionPath = SystemConfig.ZK_REGION_PATH + "/" + regionId;
        this.masterClient = new MasterClient("localhost", 8080, regionId);
        this.scheduler = Executors.newScheduledThreadPool(2);
        startScheduledTasks();
        logger.info("RegionServiceImpl初始化完成: regionId={}, regionPath={}", regionId, regionPath);
    }

    private void startScheduledTasks() {
        // 启动心跳任务
        scheduler.scheduleAtFixedRate(() -> {
            try {
                masterClient.sendHeartbeat();
            } catch (Exception e) {
                logger.error("Failed to send heartbeat", e);
            }
        }, 0, HEARTBEAT_INTERVAL, TimeUnit.SECONDS);

        // 启动状态报告任务
        scheduler.scheduleAtFixedRate(() -> {
            try {
                reportStatus();
            } catch (Exception e) {
                logger.error("Failed to report status", e);
            }
        }, 0, STATUS_REPORT_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public boolean register(RegionInfo regionInfo) {
        try {
            logger.info("开始注册区域节点: {}", regionInfo);
            // 在ZooKeeper中创建区域节点
            zkUtil.createPath(regionPath, org.apache.zookeeper.CreateMode.EPHEMERAL);
            // 存储区域信息
            zkUtil.setData(regionPath, objectMapper.writeValueAsBytes(regionInfo));
            logger.info("区域节点注册成功: {}", regionId);
            return true;
        } catch (Exception e) {
            logger.error("区域节点注册失败: {}, 错误信息: {}", regionId, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean heartbeat(String regionId) {
        try {
            logger.debug("开始更新区域节点心跳: {}", regionId);
            if (!zkUtil.exists(regionPath)) {
                logger.error("区域节点不存在: {}", regionId);
                return false;
            }
            // 更新心跳时间
            RegionInfo regionInfo = getRegionInfo(regionId);
            if (regionInfo != null) {
                regionInfo.setLastHeartbeat(System.currentTimeMillis());
                zkUtil.setData(regionPath, objectMapper.writeValueAsBytes(regionInfo));
                logger.debug("区域节点心跳更新成功: {}", regionId);
                return true;
            }
            logger.error("获取区域节点信息失败: {}", regionId);
            return false;
        } catch (Exception e) {
            logger.error("区域节点心跳更新失败: {}, 错误信息: {}", regionId, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public RegionInfo getRegionInfo(String regionId) {
        try {
            logger.debug("开始获取区域节点信息: {}", regionId);
            byte[] data = zkUtil.getData(regionPath);
            if (data != null) {
                RegionInfo regionInfo = objectMapper.readValue(data, RegionInfo.class);
                logger.debug("获取区域节点信息成功: {}", regionInfo);
                return regionInfo;
            }
            logger.warn("区域节点数据为空: {}", regionId);
            return null;
        } catch (Exception e) {
            logger.error("获取区域节点信息失败: {}, 错误信息: {}", regionId, e.getMessage(), e);
            return null;
        }
    }

    @Override
    public List<RegionInfo> getAllRegions() {
        try {
            logger.debug("开始获取所有区域节点");
            List<String> regionIds = zkUtil.getChildren(SystemConfig.ZK_REGION_PATH);
            logger.debug("获取到区域节点列表: {}", regionIds);
            List<RegionInfo> regions = new ArrayList<>();
            for (String id : regionIds) {
                RegionInfo region = getRegionInfo(id);
                if (region != null) {
                    regions.add(region);
                }
            }
            logger.debug("获取所有区域节点成功: {}", regions);
            return regions;
        } catch (Exception e) {
            logger.error("获取所有区域节点失败, 错误信息: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    @Override
    public boolean createTable(TableInfo tableInfo) {
        try {
            logger.info("开始创建表: {}", tableInfo);
            // 检查表是否已存在
            if (getTableInfo(tableInfo.getTableName()) != null) {
                logger.warn("表已存在: {}", tableInfo.getTableName());
                return true;
            }
            
            // 创建表
            String createTableSQL = generateCreateTableSQL(tableInfo);
            logger.debug("创建表SQL: {}", createTableSQL);
            boolean success = MySQLUtil.executeUpdate(createTableSQL);
            if (success) {
                // 在ZooKeeper中记录表信息
                String tablePath = SystemConfig.ZK_TABLE_PATH + "/" + tableInfo.getTableName();
                logger.debug("创建ZooKeeper表节点: {}", tablePath);
                zkUtil.createPath(tablePath, org.apache.zookeeper.CreateMode.PERSISTENT);
                zkUtil.setData(tablePath, objectMapper.writeValueAsBytes(tableInfo));
                logger.info("表创建成功: {}", tableInfo.getTableName());
                return true;
            }
            logger.error("MySQL执行创建表失败: {}", tableInfo.getTableName());
            return false;
        } catch (Exception e) {
            logger.error("创建表失败: {}, 错误信息: {}", tableInfo.getTableName(), e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean dropTable(String tableName) {
        try {
            logger.info("开始删除表: {}", tableName);
            // 检查表是否存在
            if (getTableInfo(tableName) == null) {
                logger.warn("表不存在: {}", tableName);
                return true;
            }
            
            // 删除表
            String dropTableSQL = "DROP TABLE IF EXISTS " + tableName;
            logger.debug("删除表SQL: {}", dropTableSQL);
            boolean success = MySQLUtil.executeUpdate(dropTableSQL);
            if (success) {
                // 从ZooKeeper中删除表信息
                String tablePath = SystemConfig.ZK_TABLE_PATH + "/" + tableName;
                logger.debug("删除ZooKeeper表节点: {}", tablePath);
                zkUtil.delete(tablePath);
                logger.info("表删除成功: {}", tableName);
                return true;
            }
            logger.error("MySQL执行删除表失败: {}", tableName);
            return false;
        } catch (Exception e) {
            logger.error("删除表失败: {}, 错误信息: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public TableInfo getTableInfo(String tableName) {
        try {
            logger.debug("开始获取表信息: {}", tableName);
            String tablePath = SystemConfig.ZK_TABLE_PATH + "/" + tableName;
            byte[] data = zkUtil.getData(tablePath);
            if (data != null) {
                TableInfo tableInfo = objectMapper.readValue(data, TableInfo.class);
                logger.debug("获取表信息成功: {}", tableInfo);
                return tableInfo;
            }
            logger.warn("表数据为空: {}", tableName);
            return null;
        } catch (Exception e) {
            logger.error("获取表信息失败: {}, 错误信息: {}", tableName, e.getMessage(), e);
            return null;
        }
    }

    @Override
    public List<TableInfo> getTablesByRegion(String regionId) {
        try {
            logger.debug("开始获取区域节点上的表: {}", regionId);
            List<String> tableNames = zkUtil.getChildren(SystemConfig.ZK_TABLE_PATH);
            logger.debug("获取到表名列表: {}", tableNames);
            List<TableInfo> tables = new ArrayList<>();
            for (String tableName : tableNames) {
                TableInfo tableInfo = getTableInfo(tableName);
                if (tableInfo != null && tableInfo.getRegionId().equals(regionId)) {
                    tables.add(tableInfo);
                }
            }
            logger.debug("获取区域节点上的表成功: {}", tables);
            return tables;
        } catch (Exception e) {
            logger.error("获取区域节点上的表失败: {}, 错误信息: {}", regionId, e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    private String generateCreateTableSQL(TableInfo tableInfo) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableInfo.getTableName()).append(" (");
        
        // 添加列定义
        for (String column : tableInfo.getColumns()) {
            sql.append(column).append(" VARCHAR(255), ");
        }
        
        // 添加主键
        sql.append("PRIMARY KEY (").append(tableInfo.getPrimaryKey()).append(")");
        sql.append(")");
        
        return sql.toString();
    }

    @Override
    public boolean insert(String tableName, Map<String, Object> data) {
        try {
            logger.info("开始插入数据到表: {}, 数据: {}", tableName, data);
            TableInfo tableInfo = getTableInfo(tableName);
            if (tableInfo == null) {
                logger.error("表不存在: {}", tableName);
                return false;
            }

            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO ").append(tableName).append(" (");
            sql.append(String.join(", ", data.keySet()));
            sql.append(") VALUES (");
            sql.append(String.join(", ", Collections.nCopies(data.size(), "?")));
            sql.append(")");

            List<Object> params = new ArrayList<>(data.values());
            boolean success = MySQLUtil.executeUpdate(sql.toString(), params.toArray());
            logger.info("数据插入{}: {}", success ? "成功" : "失败", tableName);
            return success;
        } catch (Exception e) {
            logger.error("插入数据失败: {}, 错误信息: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean batchInsert(String tableName, List<Map<String, Object>> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            logger.warn("批量插入数据为空: {}", tableName);
            return false;
        }

        try {
            logger.info("开始批量插入数据到表: {}, 数据条数: {}", tableName, dataList.size());
            TableInfo tableInfo = getTableInfo(tableName);
            if (tableInfo == null) {
                logger.error("表不存在: {}", tableName);
                return false;
            }

            // 获取所有列名
            Set<String> columns = dataList.get(0).keySet();
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO ").append(tableName).append(" (");
            sql.append(String.join(", ", columns));
            sql.append(") VALUES ");

            // 构建批量插入的SQL
            List<String> valuePlaceholders = Collections.nCopies(columns.size(), "?");
            String rowPlaceholder = "(" + String.join(", ", valuePlaceholders) + ")";
            sql.append(String.join(", ", Collections.nCopies(dataList.size(), rowPlaceholder)));

            // 收集所有参数
            List<Object> params = new ArrayList<>();
            for (Map<String, Object> data : dataList) {
                for (String column : columns) {
                    params.add(data.get(column));
                }
            }

            boolean success = MySQLUtil.executeUpdate(sql.toString(), params.toArray());
            logger.info("批量数据插入{}: {}", success ? "成功" : "失败", tableName);
            return success;
        } catch (Exception e) {
            logger.error("批量插入数据失败: {}, 错误信息: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean update(String tableName, Map<String, Object> data, String whereClause) {
        try {
            logger.info("开始更新表数据: {}, 数据: {}, 条件: {}", tableName, data, whereClause);
            TableInfo tableInfo = getTableInfo(tableName);
            if (tableInfo == null) {
                logger.error("表不存在: {}", tableName);
                return false;
            }

            StringBuilder sql = new StringBuilder();
            sql.append("UPDATE ").append(tableName).append(" SET ");
            sql.append(data.keySet().stream()
                    .map(key -> key + " = ?")
                    .collect(Collectors.joining(", ")));
            if (whereClause != null && !whereClause.trim().isEmpty()) {
                sql.append(" WHERE ").append(whereClause);
            }

            List<Object> params = new ArrayList<>(data.values());
            boolean success = MySQLUtil.executeUpdate(sql.toString(), params.toArray());
            logger.info("数据更新{}: {}", success ? "成功" : "失败", tableName);
            return success;
        } catch (Exception e) {
            logger.error("更新数据失败: {}, 错误信息: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean delete(String tableName, String whereClause) {
        try {
            logger.info("开始删除表数据: {}, 条件: {}", tableName, whereClause);
            TableInfo tableInfo = getTableInfo(tableName);
            if (tableInfo == null) {
                logger.error("表不存在: {}", tableName);
                return false;
            }

            StringBuilder sql = new StringBuilder();
            sql.append("DELETE FROM ").append(tableName);
            if (whereClause != null && !whereClause.trim().isEmpty()) {
                sql.append(" WHERE ").append(whereClause);
            }

            boolean success = MySQLUtil.executeUpdate(sql.toString());
            logger.info("数据删除{}: {}", success ? "成功" : "失败", tableName);
            return success;
        } catch (Exception e) {
            logger.error("删除数据失败: {}, 错误信息: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public List<Map<String, Object>> query(String tableName, List<String> columns, String whereClause) {
        try {
            logger.info("开始查询表数据: {}, 列: {}, 条件: {}", tableName, columns, whereClause);
            TableInfo tableInfo = getTableInfo(tableName);
            if (tableInfo == null) {
                logger.error("表不存在: {}", tableName);
                return new ArrayList<>();
            }

            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ");
            sql.append(columns == null || columns.isEmpty() ? "*" : String.join(", ", columns));
            sql.append(" FROM ").append(tableName);
            if (whereClause != null && !whereClause.trim().isEmpty()) {
                sql.append(" WHERE ").append(whereClause);
            }

            List<Object[]> results = MySQLUtil.executeQuery(sql.toString());
            List<Map<String, Object>> records = new ArrayList<>();
            for (Object[] row : results) {
                Map<String, Object> record = new HashMap<>();
                for (int i = 0; i < row.length; i++) {
                    String columnName = columns != null && i < columns.size() ? 
                            columns.get(i) : "column" + (i + 1);
                    record.put(columnName, row[i]);
                }
                records.add(record);
            }
            logger.info("查询完成，返回记录数: {}", records.size());
            return records;
        } catch (Exception e) {
            logger.error("查询数据失败: {}, 错误信息: {}", tableName, e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    @Override
    public List<Map<String, Object>> queryWithPagination(String tableName, List<String> columns,
                                                       String whereClause, int pageNum, int pageSize) {
        try {
            logger.info("开始分页查询表数据: {}, 列: {}, 条件: {}, 页码: {}, 每页大小: {}", 
                    tableName, columns, whereClause, pageNum, pageSize);
            TableInfo tableInfo = getTableInfo(tableName);
            if (tableInfo == null) {
                logger.error("表不存在: {}", tableName);
                return new ArrayList<>();
            }

            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ");
            sql.append(columns == null || columns.isEmpty() ? "*" : String.join(", ", columns));
            sql.append(" FROM ").append(tableName);
            if (whereClause != null && !whereClause.trim().isEmpty()) {
                sql.append(" WHERE ").append(whereClause);
            }
            sql.append(" LIMIT ").append((pageNum - 1) * pageSize).append(", ").append(pageSize);

            List<Object[]> results = MySQLUtil.executeQuery(sql.toString());
            List<Map<String, Object>> records = new ArrayList<>();
            for (Object[] row : results) {
                Map<String, Object> record = new HashMap<>();
                for (int i = 0; i < row.length; i++) {
                    String columnName = columns != null && i < columns.size() ? 
                            columns.get(i) : "column" + (i + 1);
                    record.put(columnName, row[i]);
                }
                records.add(record);
            }
            logger.info("分页查询完成，返回记录数: {}", records.size());
            return records;
        } catch (Exception e) {
            logger.error("分页查询数据失败: {}, 错误信息: {}", tableName, e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    @Override
    public long count(String tableName, String whereClause) {
        try {
            logger.info("开始统计表记录数: {}, 条件: {}", tableName, whereClause);
            TableInfo tableInfo = getTableInfo(tableName);
            if (tableInfo == null) {
                logger.error("表不存在: {}", tableName);
                return 0;
            }

            StringBuilder sql = new StringBuilder();
            sql.append("SELECT COUNT(*) FROM ").append(tableName);
            if (whereClause != null && !whereClause.trim().isEmpty()) {
                sql.append(" WHERE ").append(whereClause);
            }

            List<Object[]> results = MySQLUtil.executeQuery(sql.toString());
            if (!results.isEmpty() && results.get(0).length > 0) {
                Object count = results.get(0)[0];
                if (count instanceof Number) {
                    return ((Number) count).longValue();
                }
            }
            return 0;
        } catch (Exception e) {
            logger.error("统计记录数失败: {}, 错误信息: {}", tableName, e.getMessage(), e);
            return 0;
        }
    }

    @Override
    public boolean execute(String sql) {
        try {
            logger.info("开始执行SQL: {}", sql);
            boolean success = MySQLUtil.executeUpdate(sql);
            logger.info("SQL执行{}", success ? "成功" : "失败");
            return success;
        } catch (Exception e) {
            logger.error("执行SQL失败: {}, 错误信息: {}", sql, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public List<Map<String, Object>> executeQuery(String sql) {
        try {
            logger.info("开始执行查询SQL: {}", sql);
            List<Object[]> results = MySQLUtil.executeQuery(sql);
            List<Map<String, Object>> records = new ArrayList<>();
            for (Object[] row : results) {
                Map<String, Object> record = new HashMap<>();
                for (int i = 0; i < row.length; i++) {
                    record.put("column" + (i + 1), row[i]);
                }
                records.add(record);
            }
            logger.info("查询SQL执行完成，返回记录数: {}", records.size());
            return records;
        } catch (Exception e) {
            logger.error("执行查询SQL失败: {}, 错误信息: {}", sql, e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    @Override
    public boolean reportStatus() {
        try {
            RegionInfo status = new RegionInfo();
            status.setRegionId(regionId);
            status.setStatus("ACTIVE");
            status.setLastHeartbeat(System.currentTimeMillis());
            return masterClient.reportStatus(status);
        } catch (Exception e) {
            logger.error("Failed to report status", e);
            return false;
        }
    }

    @Override
    public boolean updateRouteInfo() {
        try {
            Map<String, Object> routeInfo = masterClient.getRouteInfo();
            // 更新本地路由信息
            if (routeInfo != null && !routeInfo.isEmpty()) {
                // 更新表路由信息
                @SuppressWarnings("unchecked")
                Map<String, String> tableRoutes = (Map<String, String>) routeInfo.get("tableRoutes");
                if (tableRoutes != null) {
                    // 更新本地表路由缓存
                    for (Map.Entry<String, String> entry : tableRoutes.entrySet()) {
                        String tableName = entry.getKey();
                        String targetRegionId = entry.getValue();
                        // 如果表不在当前Region，需要迁移数据
                        if (!targetRegionId.equals(regionId)) {
                            requestDataMigration(regionId, targetRegionId, tableName);
                        }
                    }
                }
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.error("Failed to update route info", e);
            return false;
        }
    }

    @Override
    public boolean reportTableDistribution() {
        try {
            // 获取本地表信息
            List<TableInfo> tables = getTablesByRegion(regionId);
            return masterClient.reportTableDistribution(tables);
        } catch (Exception e) {
            logger.error("Failed to report table distribution", e);
            return false;
        }
    }

    @Override
    public boolean handleMasterCommand(String command) {
        try {
            return masterClient.handleMasterCommand(command);
        } catch (Exception e) {
            logger.error("Failed to handle master command", e);
            return false;
        }
    }

    @Override
    public boolean requestDataMigration(String sourceRegionId, String targetRegionId, String tableName) {
        try {
            return masterClient.requestDataMigration(sourceRegionId, targetRegionId, tableName);
        } catch (Exception e) {
            logger.error("Failed to request data migration", e);
            return false;
        }
    }

    @Override
    public boolean executeDataMigration(String sourceRegionId, String targetRegionId, String tableName) {
        try {
            logger.info("开始执行数据迁移: 从{}到{}, 表: {}", sourceRegionId, targetRegionId, tableName);
            
            // 1. 获取表信息
            TableInfo tableInfo = getTableInfo(tableName);
            if (tableInfo == null) {
                logger.error("表不存在: {}", tableName);
                return false;
            }

            // 2. 如果是源Region，导出数据
            if (sourceRegionId.equals(regionId)) {
                // 分批读取数据
                int batchSize = 1000;
                int offset = 0;
                while (true) {
                    List<Map<String, Object>> batchData = queryWithPagination(
                        tableName, null, null, offset / batchSize + 1, batchSize);
                    
                    if (batchData.isEmpty()) {
                        break;
                    }

                    // 发送数据到目标Region
                    // TODO: 实现数据发送逻辑
                    logger.info("导出数据批次: {}, 大小: {}", offset / batchSize + 1, batchData.size());
                    
                    offset += batchSize;
                }
            }
            
            // 3. 如果是目标Region，导入数据
            if (targetRegionId.equals(regionId)) {
                // TODO: 实现数据接收和导入逻辑
                logger.info("准备接收数据");
            }

            logger.info("数据迁移完成");
            return true;
        } catch (Exception e) {
            logger.error("数据迁移失败: {}", e.getMessage(), e);
            return false;
        }
    }

    // 关闭服务
    public void shutdown() {
        try {
            scheduler.shutdown();
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
} 