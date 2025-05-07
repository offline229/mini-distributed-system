package com.mds.common.model;

import java.io.Serializable;
import java.util.List;

public class TableInfo implements Serializable {
    private String tableName;
    private String regionId;
    private List<String> columns;
    private String primaryKey;
    private long createTime;
    private String status; // ACTIVE, INACTIVE

    public TableInfo() {
    }

    public TableInfo(String tableName, String regionId, List<String> columns, String primaryKey) {
        this.tableName = tableName;
        this.regionId = regionId;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.createTime = System.currentTimeMillis();
        this.status = "ACTIVE";
    }

    // Getters and Setters
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "tableName='" + tableName + '\'' +
                ", regionId='" + regionId + '\'' +
                ", columns=" + columns +
                ", primaryKey='" + primaryKey + '\'' +
                ", createTime=" + createTime +
                ", status='" + status + '\'' +
                '}';
    }
} 