package com.mds.master.self;

import com.mds.common.RegionInfo;
import com.mds.common.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mds.master.RegionWatcher; // 需要导入 RegionWatcher

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap; // 用于并发访问内存元数据
import java.util.regex.Matcher;
import java.util.regex.Pattern; // 用于解析 DDL

public class MetaManager {
    private static final Logger logger = LoggerFactory.getLogger(MetaManager.class);

    // 需要 RegionWatcher 来获取当前在线的 RegionServer 信息
    private final RegionWatcher regionWatcher;

    // 内存中的表元数据（简化实现）
    // 存储表名 -> 分片键列名 的映射 (模拟)
    private final Map<String, String> tableShardingKeys = new ConcurrentHashMap<>();
    // 存储表名 -> 表是否存在 的状态 (模拟)
    private final Map<String, Boolean> tableExists = new ConcurrentHashMap<>();


    // Regex for basic DDL parsing
    private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(
            "^CREATE\\s+TABLE\\s+IF\\s+NOT\\s+EXISTS\\s+([\\w_]+).*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    private static final Pattern SIMPLE_CREATE_TABLE_PATTERN = Pattern.compile(
            "^CREATE\\s+TABLE\\s+([\\w_]+).*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    private static final Pattern DROP_TABLE_PATTERN = Pattern.compile(
            "^DROP\\s+TABLE\\s+IF\\s+EXISTS\\s+([\\w_]+).*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    private static final Pattern SIMPLE_DROP_TABLE_PATTERN = Pattern.compile(
            "^DROP\\s+TABLE\\s+([\\w_]+).*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    private static final Pattern ALTER_TABLE_PATTERN = Pattern.compile(
            "^ALTER\\s+TABLE\\s+([\\w_]+).*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );


    // 构造函数，接收 RegionWatcher 实例
    public MetaManager(RegionWatcher regionWatcher) {
        this.regionWatcher = regionWatcher;
    }

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
        // 加载持久化的表模式和分片信息到内存
        loadMetadataFromPersistentStore();
    }

    // 用于 MasterDispatcher 处理 DDL 时更新 Master 的元数据
    public boolean updateMetadata(String sql) {
        String lowerSql = sql.trim().toLowerCase();
        String tableName = null;
        boolean success = false;

        try {
            // 简单的 DDL 解析和元数据更新（内存中）
            Matcher createMatcher = CREATE_TABLE_PATTERN.matcher(sql.trim());
            if (!createMatcher.matches()) { // Try without IF NOT EXISTS
                createMatcher = SIMPLE_CREATE_TABLE_PATTERN.matcher(sql.trim());
            }

            Matcher dropMatcher = DROP_TABLE_PATTERN.matcher(sql.trim());
            if (!dropMatcher.matches()) { // Try without IF EXISTS
                dropMatcher = SIMPLE_DROP_TABLE_PATTERN.matcher(sql.trim());
            }

            Matcher alterMatcher = ALTER_TABLE_PATTERN.matcher(sql.trim());


            if (createMatcher.matches()) {
                tableName = createMatcher.group(1).toLowerCase();
                // 模拟创建表，设置存在状态和默认分片键
                tableExists.put(tableName, true);
                // TODO: 更智能地识别分片键，或者从外部配置读取
                tableShardingKeys.put(tableName, "id"); // 假设 'id' 是分片键
                logger.info("MetaManager 更新: 表 '{}' 已创建 (内存模拟)", tableName);
                success = true;
            } else if (dropMatcher.matches()) {
                tableName = dropMatcher.group(1).toLowerCase();
                // 模拟删除表，移除存在状态和分片键信息
                tableExists.remove(tableName);
                tableShardingKeys.remove(tableName);
                logger.info("MetaManager 更新: 表 '{}' 已删除 (内存模拟)", tableName);
                success = true;
            } else if (alterMatcher.matches()) {
                tableName = alterMatcher.group(1).toLowerCase();
                // TODO: 模拟 ALTER TABLE 的元数据更新，根据 ALTER 语句修改 tableShardingKeys 或其他模式信息
                logger.warn("MetaManager 收到 ALTER TABLE 语句，但只进行了表名识别，未执行详细元数据更新: {}", sql);
                // 简单标记为成功，但实际未处理 ALTER 细节
                success = true;
            } else {
                logger.warn("MetaManager 无法解析为 CREATE/DROP/ALTER TABLE 语句，元数据未更新: {}", sql);
                // 如果是其他 DDL 类型（如 CREATE INDEX, DROP INDEX 等），这里也需要处理其元数据影响
                // 对于无法识别的 DDL，认为元数据更新失败
                success = false;
            }

            // TODO: 将元数据变更持久化到 MySQL 或其他存储
            if(success) { // 仅在内存更新成功后尝试持久化
                persistMetadataToPersistentStore();
            }


        } catch (Exception e) {
            logger.error("MetaManager 更新元数据失败，SQL: {}", sql, e);
            success = false;
        }
        return success;
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
            logger.info("Region信息已保存到 MySQL (旧方法): {}", info.getRegionId());
        } else {
            logger.warn("Region信息保存到 MySQL 失败 (旧方法): {}", info.getRegionId());
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

    // 更新 Region 的负载状态（Master 收到心跳时调用）
    public static boolean updateRegionStatus(String regionId, int load) {
        String sql = "UPDATE regions SET load = ? WHERE region_id = ?";
        boolean success = MySQLUtil.executeUpdate(sql, load, regionId);
        if (success) {
            logger.debug("更新 Region [{}] 负载到 MySQL: {}", regionId, load);
        } else {
            logger.warn("更新 Region [{}] 负载到 MySQL 失败: {}", regionId, load);
        }
        return success;
    }

    // TODO: 实现从持久化存储加载表模式和分片信息的逻辑
    private void loadMetadataFromPersistentStore() {
        logger.info("加载表模式和分片信息从持久化存储 (TODO)");
        // 示例：从 MySQL 表（需要先创建）加载表名、分片键等信息
        // 需要创建额外的表，例如 'table_schemas' 和 'sharding_maps'
        // String sql = "SELECT table_name, sharding_key_column FROM table_schemas";
        // try {
        //      List<Object[]> rows = MySQLUtil.executeQuery(sql);
        //      for (Object[] row : rows) {
        //          String tableName = (String) row[0];
        //          String shardingKeyColumn = (String) row[1];
        //          tableExists.put(tableName, true);
        //          if (shardingKeyColumn != null && !shardingKeyColumn.isEmpty()) {
        //             tableShardingKeys.put(tableName, shardingKeyColumn);
        //          }
        //          logger.info("加载表元数据: 表 '{}', 分片键 '{}'", tableName, shardingKeyColumn);
        //      }
        // } catch (SQLException e) {
        //      logger.error("加载表元数据失败", e);
        // }

        // TODO: 加载分片映射信息 (shardId/range -> RegionId)
        // 例如：从一个 sharding_map 表加载
        // String shardingMapSql = "SELECT table_name, shard_range_start, shard_range_end, region_id FROM sharding_maps";
        // ... 加载并存储在一个 Map 中 (例如 Map<String, List<ShardMapEntry>>)
    }

    // TODO: 实现将内存中的表模式和分片信息持久化到 MySQL 或其他存储的逻辑
    private void persistMetadataToPersistentStore() {
        logger.info("持久化表模式和分片信息到持久化存储 (TODO)");
        // 在 updateMetadata() 成功更新内存后调用此方法
        // 需要将 tableShardingKeys 和 tableExists 的信息（以及更完整的模式信息）写入持久化存储
        // 例如，删除旧的表记录，插入新的表记录
        // 需要处理并发写的情况，确保持久化过程的原子性和一致性
    }
}
