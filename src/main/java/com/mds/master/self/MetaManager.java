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
                host VARCHAR(64),
                port INT,
                load INT,
                createTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """;
        boolean result = MySQLUtil.executeUpdate(sql);
        if (result) {
            logger.info("Region元数据表初始化成功");
        } else {
            logger.warn("Region元数据表初始化失败");
        }
    }

    //注册regioninfo到mysql中，元数据持久化
    public void saveRegionInfo(RegionInfo info) {
        String sql = """
            REPLACE INTO regions (region_id, host, port, load, createTime)
            VALUES (?, ?, ?, ? ,?)
        """;
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
        List<RegionInfo> list = new ArrayList<>();
        String sql = "SELECT region_id, host, port, load createTime FROM regions";
        List<Object[]> rows = MySQLUtil.executeQuery(sql);
        for (Object[] row : rows) {
            RegionInfo info = new RegionInfo(
                    (String) row[0],
                    (String) row[1],
                    ((Number) row[2]).intValue(),
                    ((Number) row[3]).intValue(),
                    (long) row[4]
            );
            list.add(info);
        }
        return list;
    }
}
