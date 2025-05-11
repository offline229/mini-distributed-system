package com.mds.master.self;

import com.mds.common.RegionInfo;
import com.mds.common.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MetaManager {
    private static final Logger logger = LoggerFactory.getLogger(MetaManager.class);

    // 建表RegionInfo
    public void init() {
        String sql = """
            CREATE TABLE IF NOT EXISTS regions (
                region_id VARCHAR(64) PRIMARY KEY,
                host VARCHAR(100) NOT NULL,
                port INT NOT NULL,
                load INT DEFAULT 0,
                createTime BIGINT NOT NULL,
            )
        """;
        try {
            MySQLUtil.execute(sql);
            System.out.println("表 regions 初始化完成");
        } catch (SQLException e) {
            System.err.println("初始化表 regions 失败：" + e.getMessage());
        }
    }

    //注册regioninfo到mysql中，元数据持久化
    public void saveRegionInfo(RegionInfo info) {
        String sql = "INSERT INTO regions (region_id, host, port, load, createTime) " +
                "VALUES (?, ?, ?, ?, ?)";
        boolean result = MySQLUtil.executeUpdate(sql,
                info.getRegionId(),
                info.getHost(),
                info.getPort(),
                info.getLoad(),
                info.getCreateTime());

        if (result) {
            logger.info("Region信息已保存: {}", info.getRegionId());
        } else {
            logger.warn("Region信息保存失败: {}", info.getRegionId());
        }
    }

    public List<RegionInfo> loadAllRegions() throws SQLException {
        List<RegionInfo> result = new ArrayList<>();
        String sql = "SELECT region_id, host, port, load, createTime FROM regions";
        List<Object[]> rows = MySQLUtil.executeQuery(sql);

        for (Object[] row : rows) {
            RegionInfo region = new RegionInfo();
            region.setRegionId((String) row[0]);
            region.setHost((String) row[1]);
            region.setPort((Integer) row[2]);
            region.setLoad((Integer) row[3]);
            region.setCreateTime((Long) row[4]);
            result.add(region);
        }

        return result;
    }

    //上下线可以更新
    public static boolean updateRegionStatus(String regionId, int load) {
        String sql = "UPDATE regions SET load = ? WHERE region_id = ?";
        return MySQLUtil.executeUpdate(sql, load, regionId);
    }

}
