package com.mds.common.service;

import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import java.util.List;
import java.util.Map;

public interface RegionService {
    // 区域节点注册
    boolean register(RegionInfo regionInfo);
    
    // 区域节点心跳
    boolean heartbeat(String regionId);
    
    // 获取区域节点信息
    RegionInfo getRegionInfo(String regionId);
    
    // 获取所有区域节点
    List<RegionInfo> getAllRegions();
    
    // 创建表
    boolean createTable(TableInfo tableInfo);
    
    // 删除表
    boolean dropTable(String tableName);
    
    // 获取表信息
    TableInfo getTableInfo(String tableName);
    
    // 获取区域节点上的所有表
    List<TableInfo> getTablesByRegion(String regionId);

    // 数据操作相关方法
    // 插入数据
    boolean insert(String tableName, Map<String, Object> data);
    
    // 批量插入数据
    boolean batchInsert(String tableName, List<Map<String, Object>> dataList);
    
    // 更新数据
    boolean update(String tableName, Map<String, Object> data, String whereClause);
    
    // 删除数据
    boolean delete(String tableName, String whereClause);
    
    // 查询数据
    List<Map<String, Object>> query(String tableName, List<String> columns, String whereClause);
    
    // 分页查询
    List<Map<String, Object>> queryWithPagination(String tableName, List<String> columns, 
                                                String whereClause, int pageNum, int pageSize);
    
    // 获取记录总数
    long count(String tableName, String whereClause);
    
    // 执行自定义SQL
    boolean execute(String sql);
    
    // 执行查询SQL
    List<Map<String, Object>> executeQuery(String sql);
} 