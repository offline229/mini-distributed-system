package com.mds.master.self;

import com.mds.common.RegionServerInfo;
import com.mds.common.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mds.master.RegionWatcher; // 需要导入 RegionWatcher

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap; // 用于并发访问内存元数据
import java.util.regex.Matcher;
import java.util.regex.Pattern; // 用于解析 DDL

public class MetaManager {
    // 需要 RegionWatcher 来获取当前在线的 RegionServer 信息
    private final RegionWatcher regionWatcher;

    // 构造函数，接收 RegionWatcher 实例
    public MetaManager(RegionWatcher regionWatcher) {
        this.regionWatcher = regionWatcher;
    }

    // 建表RegionInfo
    public void init() {
        String sql = """
                    CREATE TABLE region_servers (
                                regionserver_id VARCHAR(64) NOT NULL,
                                replica_key VARCHAR(64) NOT NULL,
                                host VARCHAR(128) NOT NULL,
                                port INT NOT NULL,
                                status VARCHAR(32) NOT NULL,
                                connections INT NOT NULL,
                                PRIMARY KEY (regionserver_id, host, port)
                            );
                """;
        try {
            MySQLUtil.execute(sql);
            System.out.println("表 regions 初始化完成");
        } catch (SQLException e) {
            System.err.println("初始化表 regions 失败：" + e.getMessage());
        }
        // 加载持久化的表模式和分片信息到内存
        // loadMetadataFromPersistentStore();
    }

    // 注册regioninfo到mysql中，元数据持久化
    public void saveRegionInfo(RegionServerInfo regionServerInfo) throws SQLException {
        String sql = "INSERT INTO region_servers (regionserver_id, replica_key, host, port, status, connections) VALUES (?, ?, ?, ?, ?, ?)";

        for (RegionServerInfo.HostPortStatus hostPortStatus : regionServerInfo.getHostsPortsStatusList()) {
            MySQLUtil.executeUpdate(sql,
                    regionServerInfo.getRegionserverID(),
                    regionServerInfo.getReplicaKey(),
                    hostPortStatus.getHost(),
                    hostPortStatus.getPort(),
                    hostPortStatus.getStatus(),
                    hostPortStatus.getConnections());
        }
    }

    // 更新一个 RegionServer 的信息到数据库
    public void updateRegionInfo(RegionServerInfo regionServerInfo) throws SQLException {
        String deleteSql = "DELETE FROM region_servers WHERE regionserver_id = ?";
        String insertSql = "INSERT INTO region_servers (regionserver_id, replica_key, host, port, status, connections) VALUES (?, ?, ?, ?, ?, ?)";

        // 删除旧数据
        MySQLUtil.executeUpdate(deleteSql, regionServerInfo.getRegionserverID());

        // 插入新数据
        for (RegionServerInfo.HostPortStatus hostPortStatus : regionServerInfo.getHostsPortsStatusList()) {
            MySQLUtil.executeUpdate(insertSql,
                    regionServerInfo.getRegionserverID(),
                    regionServerInfo.getReplicaKey(),
                    hostPortStatus.getHost(),
                    hostPortStatus.getPort(),
                    hostPortStatus.getStatus(),
                    hostPortStatus.getConnections());
        }
    }

    // 删除一个 RegionServer 的元数据
    public void deleteRegionInfo(String regionServerId) throws SQLException {
        String sql = "DELETE FROM region_servers WHERE regionserver_id = ?";
        MySQLUtil.executeUpdate(sql, regionServerId);
    }

    // 据 RegionServer ID 获取对应的 RegionServer 信息
    public RegionServerInfo getRegionInfo(String regionServerId) throws SQLException {
        String sql = "SELECT replica_key, host, port, status, connections FROM region_servers WHERE regionserver_id = ?";
        List<Object[]> rows = MySQLUtil.executeQuery(sql, regionServerId);

        List<RegionServerInfo.HostPortStatus> hostPortStatusList = new ArrayList<>();
        String replicaKey = null;

        for (Object[] row : rows) {
            replicaKey = (String) row[0];
            String host = (String) row[1];
            int port = (int) row[2];
            String status = (String) row[3];
            int connections = (int) row[4];

            hostPortStatusList.add(new RegionServerInfo.HostPortStatus(host, port, status, connections));
        }

        if (replicaKey != null) {
            return new RegionServerInfo(regionServerId, hostPortStatusList, replicaKey, System.currentTimeMillis());
        }
        return null;
    }

    // 获取所有在线的 RegionServer 信息
    public List<RegionServerInfo> getAllRegionInfos() throws SQLException {
        String sql = "SELECT regionserver_id, replica_key, host, port, status, connections FROM region_servers";
        List<Object[]> rows = MySQLUtil.executeQuery(sql);

        Map<String, RegionServerInfo> regionMap = new HashMap<>();

        for (Object[] row : rows) {
            String regionServerId = (String) row[0];
            String replicaKey = (String) row[1];
            String host = (String) row[2];
            int port = (int) row[3];
            String status = (String) row[4];
            int connections = (int) row[5];

            RegionServerInfo regionInfo = regionMap.computeIfAbsent(regionServerId,
                    id -> new RegionServerInfo(regionServerId, new ArrayList<>(), replicaKey,
                            System.currentTimeMillis()));
            regionInfo.getHostsPortsStatusList()
                    .add(new RegionServerInfo.HostPortStatus(host, port, status, connections));
        }

        return new ArrayList<>(regionMap.values());
    }

    // TODO: 实现从持久化存储加载表模式和分片信息的逻辑
    private void loadMetadataFromPersistentStore() {
        // logger.info("加载表模式和分片信息从持久化存储 (TODO)");
        // 示例：从 MySQL 表（需要先创建）加载表名、分片键等信息
        // 需要创建额外的表，例如 'table_schemas' 和 'sharding_maps'
        // String sql = "SELECT table_name, sharding_key_column FROM table_schemas";
        // try {
        // List<Object[]> rows = MySQLUtil.executeQuery(sql);
        // for (Object[] row : rows) {
        // String tableName = (String) row[0];
        // String shardingKeyColumn = (String) row[1];
        // tableExists.put(tableName, true);
        // if (shardingKeyColumn != null && !shardingKeyColumn.isEmpty()) {
        // tableShardingKeys.put(tableName, shardingKeyColumn);
        // }
        // logger.info("加载表元数据: 表 '{}', 分片键 '{}'", tableName, shardingKeyColumn);
        // }
        // } catch (SQLException e) {
        // logger.error("加载表元数据失败", e);
        // }

        // TODO: 加载分片映射信息 (shardId/range -> RegionId)
        // 例如：从一个 sharding_map 表加载
        // String shardingMapSql = "SELECT table_name, shard_range_start,
        // shard_range_end, region_id FROM sharding_maps";
        // ... 加载并存储在一个 Map 中 (例如 Map<String, List<ShardMapEntry>>)
    }

    // TODO: 实现将内存中的表模式和分片信息持久化到 MySQL 或其他存储的逻辑
    private void persistMetadataToPersistentStore() {
        // logger.info("持久化表模式和分片信息到持久化存储 (TODO)");
        // 在 updateMetadata() 成功更新内存后调用此方法
        // 需要将 tableShardingKeys 和 tableExists 的信息（以及更完整的模式信息）写入持久化存储
        // 例如，删除旧的表记录，插入新的表记录
        // 需要处理并发写的情况，确保持久化过程的原子性和一致性
    }

}
