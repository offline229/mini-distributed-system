package com.mds.region;

import com.mds.region.handler.DBHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class Region {
    private static final Logger logger = LoggerFactory.getLogger(Region.class);

    private final RegionServer regionServer;
    private final String regionId;
    private final DBHandler dbHandler;
    private volatile boolean isRunning;

    // Region状态信息
    private int currentConnections;
    private long totalQueries;
    private double load;

    public Region(RegionServer regionServer, String regionId) {
        this.regionServer = regionServer;
        this.regionId = regionId;
        this.dbHandler = new DBHandler();
        this.currentConnections = 0;
        this.totalQueries = 0;
        this.load = 0.0;
    }

    public void start() {
        try {
            dbHandler.init();
            isRunning = true;
            logger.info("Region启动成功: {}", regionId);
        } catch (SQLException e) {
            logger.error("Region启动失败: {}", regionId, e);
            throw new RuntimeException("Region启动失败", e);
        }
    }

    public void stop() {
        if (!isRunning)
            return;

        try {
            isRunning = false;
            dbHandler.close();
            logger.info("Region停止成功: {}", regionId);
        } catch (Exception e) {
            logger.error("Region停止失败: {}", regionId, e);
        }
    }

    // 执行SQL操作
    public Object execute(String sql, Object[] params) throws Exception {
        if (!isRunning) {
            throw new IllegalStateException("Region未运行");
        }

        try {
            currentConnections++;
            totalQueries++;
            updateLoad();

            // 执行SQL并获取结果
            DBHandler.ExecuteResult result = dbHandler.execute(sql, params);
            logger.info("Region {} 执行SQL结果: {}", regionId, result);

            // 如果数据发生变更，通知RegionServer
            if (result.isDataChanged()) {
                regionServer.onDataChanged(result.getOperation(), result.getTableName(), 
                    result.getData(), result.getMessage());
            }

            return result.getData();
        } finally {
            currentConnections--;
            updateLoad();
        }
    }

    // 获取Region状态
    public RegionStatus getStatus() {
        return RegionStatus.builder()
                .regionId(regionId)
                .connections(currentConnections)
                .totalQueries(totalQueries)
                .load(load)
                .isRunning(isRunning)
                .build();
    }

    private void updateLoad() {
        // 简单的负载计算: 连接数/最大连接数
        this.load = currentConnections / 100.0; // 假设最大连接数为100
    }

    // Getters
    public String getRegionId() {
        return regionId;
    }

    public boolean isRunning() {
        return isRunning;
    }
}

// Region状态类
class RegionStatus {
    private final String regionId;
    private final int connections;
    private final long totalQueries;
    private final double load;
    private final boolean running;

    private RegionStatus(Builder builder) {
        this.regionId = builder.regionId;
        this.connections = builder.connections;
        this.totalQueries = builder.totalQueries;
        this.load = builder.load;
        this.running = builder.running;
    }

    // Builder模式
    public static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private String regionId;
        private int connections;
        private long totalQueries;
        private double load;
        private boolean running;

        public Builder regionId(String regionId) {
            this.regionId = regionId;
            return this;
        }

        public Builder connections(int connections) {
            this.connections = connections;
            return this;
        }

        public Builder totalQueries(long totalQueries) {
            this.totalQueries = totalQueries;
            return this;
        }

        public Builder load(double load) {
            this.load = load;
            return this;
        }

        public Builder isRunning(boolean running) {
            this.running = running;
            return this;
        }

        public RegionStatus build() {
            return new RegionStatus(this);
        }
    }

    // Getters
    public String getRegionId() {
        return regionId;
    }

    public int getConnections() {
        return connections;
    }

    public long getTotalQueries() {
        return totalQueries;
    }

    public double getLoad() {
        return load;
    }

    public boolean isRunning() {
        return running;
    }
}
