package com.mds.common;

public class RegionInfo {
    private String regionId;
    private String host;
    private int port;
    private int load;
    private long createTime;

    public RegionInfo() {
    }

    public RegionInfo(String regionId, String host, int port, int load, long createTime) {
        this.regionId = regionId;
        this.host = host;
        this.port = port;
        this.load = load;
        this.createTime = createTime;
    }

    // Getter / Setter
    public String getRegionId() {return regionId;}
    public void setRegionId(String regionId) {this.regionId = regionId;}

    public String getHost() {return host;}
    public void setHost(String host) {this.host = host;}

    public int getPort() {return port;}
    public void setPort(int port) {this.port = port;}

    public int getLoad() {return load;}
    public void setLoad(int load) {this.load = load;}

    public long getCreateTime() {return createTime;}
    public void setCreateTime(long createTime) {this.createTime = createTime;}

    @Override
    public String toString() {
        return String.format("RegionInfo{id=%s, host=%s, port=%d, load=%d, createTime=%d}",
                regionId, host, port, load, createTime);
    }
}
