package com.mds.region.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import com.mds.common.service.RegionService;
import com.mds.common.util.ZookeeperUtil;
import com.mds.common.util.MySQLUtil;
import com.mds.common.config.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RegionServiceImpl implements RegionService {
    private static final Logger logger = LoggerFactory.getLogger(RegionServiceImpl.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final ZookeeperUtil zkUtil;
    private final String regionId;
    private final String regionPath;

    public RegionServiceImpl(String regionId) throws Exception {
        this.regionId = regionId;
        this.zkUtil = new ZookeeperUtil();
        this.regionPath = SystemConfig.ZK_REGION_PATH + "/" + regionId;
        logger.info("RegionServiceImpl初始化完成: regionId={}, regionPath={}", regionId, regionPath);
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
} 