package com.mds.common;

import java.util.List;
import java.util.Objects;

public class RegionServerInfo {
    private String regionserverID; // Unique identifier for the region server
    private List<HostPortStatus> hostsPortsStatusList; // Multiple sets of host, port, status, and connections
    private String replicaKey; // Key to determine if it is a backup database
    private long createTime;

    public RegionServerInfo() {
    }

    public RegionServerInfo(String regionserverID, List<HostPortStatus> hostsPortsStatusList, String replicaKey,
            long createTime) {
        this.regionserverID = regionserverID;
        this.hostsPortsStatusList = hostsPortsStatusList;
        this.replicaKey = replicaKey;
        this.createTime = createTime;
    }

    public String getRegionserverID() {
        return regionserverID;
    }

    public void setRegionserverID(String regionserverID) {
        this.regionserverID = regionserverID;
    }

    public List<HostPortStatus> getHostsPortsStatusList() {
        return hostsPortsStatusList;
    }

    public void setHostsPortsStatusList(List<HostPortStatus> hostsPortsStatusList) {
        this.hostsPortsStatusList = hostsPortsStatusList;
    }

    public String getReplicaKey() {
        return replicaKey;
    }

    public void setReplicaKey(String replicaKey) {
        this.replicaKey = replicaKey;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    // Method to check if this is a replica
    public boolean isReplica(RegionServerInfo other) {
        return Objects.equals(this.replicaKey, other.replicaKey);
    }

    @Override
    public String toString() {
        return String.format("RegionServerInfo{id=%s, replicaKey=%s, hostsPortsStatusList=%s, createTime=%s}",
                regionserverID, replicaKey, hostsPortsStatusList, createTime);
    }

    // Nested static class to represent host, port, status, and connections
    public static class HostPortStatus {
        private String host;
        private int port;
        private String status; // New field for the status of the server
        private int connections; // New field for the number of connections

        // 添加默认构造函数
        public HostPortStatus() {
        }

        public HostPortStatus(String host, int port, String status, int connections) {
            this.host = host;
            this.port = port;
            this.status = status;
            this.connections = connections;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public int getConnections() {
            return connections;
        }

        public void setConnections(int connections) {
            this.connections = connections;
        }

        @Override
        public String toString() {
            return String.format("HostPortStatus{host=%s, port=%d, status=%s, connections=%d}",
                    host, port, status, connections);
        }
    }
}