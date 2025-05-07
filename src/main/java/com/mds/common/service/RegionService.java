package com.mds.common.service;

import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import java.util.List;

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
} 