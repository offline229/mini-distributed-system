package com.mds.master.self;

import com.mds.common.RegionServerInfo;
import com.mds.common.util.MySQLUtil;
import com.mds.master.RegionWatcher;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaManager {
    private final RegionWatcher regionWatcher;

    // 构造函数，接收 RegionWatcher 实例
    public MetaManager(RegionWatcher regionWatcher) {
        this.regionWatcher = regionWatcher;
    }

    // 初始化表结构
    public void init() {
        String sql = """
                    CREATE TABLE IF NOT EXISTS region_servers (
                                regionserver_id VARCHAR(64) NOT NULL,
                                replica_key VARCHAR(64) NOT NULL,
                                host VARCHAR(128) NOT NULL,
                                port INT NOT NULL,
                                status VARCHAR(32) NOT NULL,
                                connections INT NOT NULL,
                                last_heartbeat_time BIGINT NOT NULL,
                                PRIMARY KEY (regionserver_id, host, port)
                            );
                """;
        try {
            MySQLUtil.execute(sql);
            System.out.println("表 region_servers 初始化完成");
        } catch (SQLException e) {
            System.err.println("初始化表 region_servers 失败：" + e.getMessage());
        }
    }

    // 保存 RegionServer 信息到数据库
    public void saveRegionInfo(RegionServerInfo regionServerInfo) throws SQLException {
        String sql = """
                INSERT INTO region_servers (regionserver_id, replica_key, host, port, status, connections, last_heartbeat_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;

        for (RegionServerInfo.HostPortStatus hostPortStatus : regionServerInfo.getHostsPortsStatusList()) {
            MySQLUtil.executeUpdate(sql,
                    regionServerInfo.getRegionserverID(),
                    regionServerInfo.getReplicaKey(),
                    hostPortStatus.getHost(),
                    hostPortStatus.getPort(),
                    hostPortStatus.getStatus(),
                    hostPortStatus.getConnections(),
                    hostPortStatus.getLastHeartbeatTime());
        }
    }

    // 更新 RegionServer 信息到数据库
    public void updateRegionInfo(RegionServerInfo regionServerInfo) throws SQLException {
        String deleteSql = "DELETE FROM region_servers WHERE regionserver_id = ?";
        String insertSql = """
                INSERT INTO region_servers (regionserver_id, replica_key, host, port, status, connections, last_heartbeat_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;

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
                    hostPortStatus.getConnections(),
                    hostPortStatus.getLastHeartbeatTime());
        }
    }

    // 删除 RegionServer 信息
    public void deleteRegionInfo(String regionServerId) throws SQLException {
        String sql = "DELETE FROM region_servers WHERE regionserver_id = ?";
        MySQLUtil.executeUpdate(sql, regionServerId);
    }

    // 根据 RegionServer ID 获取 RegionServer 信息
    public RegionServerInfo getRegionInfo(String regionServerId) throws SQLException {
        String sql = """
                SELECT replica_key, host, port, status, connections, last_heartbeat_time
                FROM region_servers
                WHERE regionserver_id = ?
                """;
        List<Object[]> rows = MySQLUtil.executeQuery(sql, regionServerId);

        List<RegionServerInfo.HostPortStatus> hostPortStatusList = new ArrayList<>();
        String replicaKey = null;

        for (Object[] row : rows) {
            replicaKey = (String) row[0];
            String host = (String) row[1];
            int port = (int) row[2];
            String status = (String) row[3];
            int connections = (int) row[4];
            long lastHeartbeatTime = (long) row[5];

            hostPortStatusList.add(new RegionServerInfo.HostPortStatus(host, port, status, connections, lastHeartbeatTime));
        }

        if (replicaKey != null) {
            return new RegionServerInfo(regionServerId, hostPortStatusList, replicaKey, System.currentTimeMillis());
        }
        return null;
    }

    // 获取所有 RegionServer 信息
    public List<RegionServerInfo> getAllRegionInfos() throws SQLException {
        String sql = """
                SELECT regionserver_id, replica_key, host, port, status, connections, last_heartbeat_time
                FROM region_servers
                """;
        List<Object[]> rows = MySQLUtil.executeQuery(sql);

        Map<String, RegionServerInfo> regionMap = new HashMap<>();

        for (Object[] row : rows) {
            String regionServerId = (String) row[0];
            String replicaKey = (String) row[1];
            String host = (String) row[2];
            int port = (int) row[3];
            String status = (String) row[4];
            int connections = (int) row[5];
            long lastHeartbeatTime = (long) row[6];

            RegionServerInfo regionInfo = regionMap.computeIfAbsent(regionServerId,
                    id -> new RegionServerInfo(regionServerId, new ArrayList<>(), replicaKey, System.currentTimeMillis()));
            regionInfo.getHostsPortsStatusList()
                    .add(new RegionServerInfo.HostPortStatus(host, port, status, connections, lastHeartbeatTime));
        }

        return new ArrayList<>(regionMap.values());
    }
}