package com.mds.common.service;

import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import java.util.List;

public interface MasterService {
    // 主节点选举
    boolean electMaster();
    
    // 检查主节点状态
    boolean isMaster();
    
    // 注册区域节点
    boolean registerRegion(RegionInfo regionInfo);
    
    // 更新区域节点状态
    boolean updateRegionStatus(String regionId, String status);
    
    // 获取所有区域节点
    List<RegionInfo> getAllRegions();
    
    // 分配表到区域节点
    boolean assignTable(TableInfo tableInfo);
    
    // 获取表分布信息
    List<TableInfo> getTableDistribution();
    
    // 负载均衡
    boolean rebalance();
} 