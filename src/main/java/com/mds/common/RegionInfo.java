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

    //生成在 ZooKeeper 中的路径，例如：/regions/region-1
    public String toZKPath() {
        return "/regions/" + regionId;
    }

    //将 RegionInfo 序列化为 JSON 字符串，方便存储到 ZK 节点内容
    public String toJson() {
//        return new Gson().toJson(this);
    }

    //从 JSON 字符串反序列化 RegionInfo 对象
    public static RegionInfo fromJson(String json) {
//        return new Gson().fromJson(json, RegionInfo.class);
    }

    @Override
    public String toString() {
        return String.format("RegionInfo{id=%s, host=%s, port=%d, load=%d, createTime=%d}",
                regionId, host, port, load, createTime);
    }
}
