package com.mds.common;

public class MasterInfo {
    private String masterID;      // master唯一标识
    private String host;          // 主机地址
    private int port;            // 服务端口
    private String status;        // 状态(active/standby)
    private long createTime;      // 创建时间

    // 无参构造函数 for Jackson
    public MasterInfo() {
    }

    public MasterInfo(String masterID, String host, int port, String status, long createTime) {
        this.masterID = masterID;
        this.host = host;
        this.port = port;
        this.status = status;
        this.createTime = createTime;
    }

    // Getters and Setters
    public String getMasterID() {
        return masterID;
    }

    public void setMasterID(String masterID) {
        this.masterID = masterID;
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

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "MasterInfo{" +
                "masterID='" + masterID + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", status='" + status + '\'' +
                ", createTime=" + createTime +
                '}';
    }
}