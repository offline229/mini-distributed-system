package com.mds.region.service;

import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import java.util.List;
import java.util.Map;

public interface RegionService {
    // ... 现有方法 ...

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
     * @param command Master下发的命令
     * @return 处理结果
     */
    boolean handleMasterCommand(String command);

    /**
     * 向Master请求数据迁移
     * @param sourceRegionId 源Region ID
     * @param targetRegionId 目标Region ID
     * @param tableName 表名
     * @return 迁移结果
     */
    boolean requestDataMigration(String sourceRegionId, String targetRegionId, String tableName);

    /**
     * 执行数据迁移
     * @param sourceRegionId 源Region ID
     * @param targetRegionId 目标Region ID
     * @param tableName 表名
     * @return 迁移结果
     */
    boolean executeDataMigration(String sourceRegionId, String targetRegionId, String tableName);
} 