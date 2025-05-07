package com.mds.common.model;

import java.io.Serializable;

public class RegionInfo implements Serializable {
    private String regionId;
    private String host;
    private int port;
    private String status; // ONLINE, OFFLINE
    private long lastHeartbeat;

    public RegionInfo() {
    }

    public RegionInfo(String regionId, String host, int port) {
        this.regionId = regionId;
        this.host = host;
        this.port = port;
        this.status = "OFFLINE";
        this.lastHeartbeat = System.currentTimeMillis();
    }

    // Getters and Setters
    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
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

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    @Override
    public String toString() {
        return "RegionInfo{" +
                "regionId='" + regionId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", status='" + status + '\'' +
                ", lastHeartbeat=" + lastHeartbeat +
                '}';
    }
} 