package com.mds.region.service;

import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import java.util.List;
import java.util.Map;

public interface RegionService {
    // ... 现有方法 ...
    /**
     * 插入数据
     * 
     * @param tableName 表名
     * @param data      数据
     * @return 插入结果
     */
    boolean insert(String tableName, Map<String, Object> data);

    /**
     * 批量插入数据
     * 
     * @param tableName 表名
     * @param dataList  数据列表
     * @return 插入结果
     */
    boolean batchInsert(String tableName, List<Map<String, Object>> dataList);

    /**
     * 更新数据
     * 
     * @param tableName   表名
     * @param data        数据
     * @param whereClause 条件语句
     * @return 更新结果
     */
    boolean update(String tableName, Map<String, Object> data, String whereClause);

    /**
     * 删除数据
     * 
     * @param tableName   表名
     * @param whereClause 条件语句
     * @return 删除结果
     */
    boolean delete(String tableName, String whereClause);

    /**
     * 查询数据
     * 
     * @param tableName   表名
     * @param columns     列名列表
     * @param whereClause 条件语句
     * @return 查询结果
     */
    List<Map<String, Object>> query(String tableName, List<String> columns, String whereClause);

    /**
     * 分页查询数据
     * 
     * @param tableName   表名
     * @param columns     列名列表
     * @param whereClause 条件语句
     * @param pageNum     页码
     * @param pageSize    每页大小
     * @return 查询结果
     */
    List<Map<String, Object>> queryWithPagination(String tableName, List<String> columns,
            String whereClause, int pageNum, int pageSize);

    /**
     * 统计记录数
     * 
     * @param tableName   表名
     * @param whereClause 条件语句
     * @return 记录数
     */
    long count(String tableName, String whereClause);

    /**
     * 执行SQL语句
     * 
     * @param sql SQL语句
     * @return 执行结果
     */
    boolean execute(String sql);

    /**
     * 执行查询SQL语句
     * 
     * @param sql SQL语句
     * @return 查询结果
     */
    List<Map<String, Object>> executeQuery(String sql);

    // 与Master交互的方法
    /**
     * 向Master报告Region状态
     */
    boolean reportStatus();

    /**
     * 从Master获取最新的路由信息
     */
    boolean updateRouteInfo();

    /**
     * 向Master报告表分布情况
     */
    boolean reportTableDistribution();

    /**
     * 处理来自Master的命令
     * 
     * @param command Master下发的命令
     * @return 处理结果
     */
    boolean handleMasterCommand(String command);

    /**
     * 向Master请求数据迁移
     * 
     * @param sourceRegionId 源Region ID
     * @param targetRegionId 目标Region ID
     * @param tableName      表名
     * @return 迁移结果
     */
    boolean requestDataMigration(String sourceRegionId, String targetRegionId, String tableName);

    /**
     * 执行数据迁移
     * 
     * @param sourceRegionId 源Region ID
     * @param targetRegionId 目标Region ID
     * @param tableName      表名
     * @return 迁移结果
     */
    boolean executeDataMigration(String sourceRegionId, String targetRegionId, String tableName);

    /**
     * 注册区域节点
     * 
     * @param regionInfo 区域信息
     * @return 注册结果
     */
    boolean register(RegionInfo regionInfo);

    /**
     * 发送心跳
     * 
     * @param regionId 区域ID
     * @return 心跳结果
     */
    boolean heartbeat(String regionId);

    /**
     * 获取区域信息
     * 
     * @param regionId 区域ID
     * @return 区域信息
     */
    RegionInfo getRegionInfo(String regionId);

    /**
     * 获取所有区域信息
     * 
     * @return 区域信息列表
     */
    List<RegionInfo> getAllRegions();

    /**
     * 创建表
     * 
     * @param tableInfo 表信息
     * @return 创建结果
     */
    boolean createTable(TableInfo tableInfo);

    /**
     * 删除表
     * 
     * @param tableName 表名
     * @return 删除结果
     */
    boolean dropTable(String tableName);

    /**
     * 获取表信息
     * 
     * @param tableName 表名
     * @return 表信息
     */
    TableInfo getTableInfo(String tableName);

    /**
     * 获取区域节点上的所有表
     * 
     * @param regionId 区域ID
     * @return 表信息列表
     */
    List<TableInfo> getTablesByRegion(String regionId);

    /**
     * 关闭区域服务
     * 用于优雅地关闭服务，清理资源
     */
    void shutdown();

}