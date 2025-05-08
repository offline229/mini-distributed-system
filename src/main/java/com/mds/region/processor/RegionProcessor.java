package com.mds.region.processor;

import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class RegionProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RegionProcessor.class);
    
    private final String regionId;
    private final ExecutorService threadPool;
    private final AtomicBoolean isRunning;
    private final Map<String, TableInfo> localTables;
    private final Map<String, RegionInfo> routeTable;

    public RegionProcessor(String regionId) {
        this.regionId = regionId;
        this.threadPool = Executors.newFixedThreadPool(5);
        this.isRunning = new AtomicBoolean(false);
        this.localTables = new ConcurrentHashMap<>();
        this.routeTable = new ConcurrentHashMap<>();
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            logger.info("Region processor started for region: {}", regionId);
        }
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            threadPool.shutdown();
            logger.info("Region processor stopped for region: {}", regionId);
        }
    }

    public void processRequest(Object request) {
        if (!isRunning.get()) {
            throw new IllegalStateException("Region processor is not running");
        }

        threadPool.submit(() -> {
            try {
                // TODO: 实现具体的请求处理逻辑
                // 1. 解析请求类型
                // 2. 根据请求类型选择处理方式
                // 3. 执行相应的业务逻辑
                // 4. 返回处理结果
            } catch (Exception e) {
                logger.error("Error processing request", e);
                // TODO: 处理异常情况
            }
        });
    }

    public void updateRouteInfo(Map<String, RegionInfo> newRouteInfo) {
        routeTable.clear();
        routeTable.putAll(newRouteInfo);
        logger.info("Route table updated for region: {}", regionId);
    }

    public void addLocalTable(TableInfo tableInfo) {
        localTables.put(tableInfo.getTableName(), tableInfo);
        logger.info("Local table added: {}", tableInfo.getTableName());
    }

    public void removeLocalTable(String tableName) {
        localTables.remove(tableName);
        logger.info("Local table removed: {}", tableName);
    }

    public Map<String, TableInfo> getLocalTables() {
        return new ConcurrentHashMap<>(localTables);
    }

    public Map<String, RegionInfo> getRouteTable() {
        return new ConcurrentHashMap<>(routeTable);
    }

    public boolean isRunning() {
        return isRunning.get();
    }
} 