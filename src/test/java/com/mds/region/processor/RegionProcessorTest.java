package com.mds.region.processor;

import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RegionProcessorTest {
    private static final String TEST_REGION_ID = "test-region-1";
    private RegionProcessor processor;
    private CountDownLatch processLatch;
    private AtomicInteger processedCount;

    @Before
    public void setUp() {
        processor = new RegionProcessor(TEST_REGION_ID);
        processLatch = new CountDownLatch(1);
        processedCount = new AtomicInteger(0);
    }

    @After
    public void tearDown() {
        if (processor != null) {
            processor.stop();
        }
    }

    @Test
    public void testProcessorStartAndStop() {
        processor.start();
        assertTrue(processor.isRunning());

        processor.stop();
        assertFalse(processor.isRunning());
    }

    @Test
    public void testConcurrentRequestProcessing() throws Exception {
        processor.start();
        assertTrue(processor.isRunning());

        int requestCount = 10;
        CountDownLatch requestLatch = new CountDownLatch(requestCount);

        // 提交多个请求
        for (int i = 0; i < requestCount; i++) {
            final int requestId = i;
            processor.processRequest(new Object() {
                @Override
                public String toString() {
                    return "Request-" + requestId;
                }
            });
            requestLatch.countDown();
        }

        // 等待所有请求处理完成
        assertTrue(requestLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testRouteTableUpdate() {
        processor.start();
        assertTrue(processor.isRunning());

        // 创建测试路由信息
        Map<String, RegionInfo> routeInfo = Map.of(
            "table1", createRegionInfo("region-1"),
            "table2", createRegionInfo("region-2")
        );

        // 更新路由表
        processor.updateRouteInfo(routeInfo);

        // 验证路由表更新
        Map<String, RegionInfo> currentRouteTable = processor.getRouteTable();
        assertEquals(2, currentRouteTable.size());
        assertTrue(currentRouteTable.containsKey("table1"));
        assertTrue(currentRouteTable.containsKey("table2"));
    }

    @Test
    public void testLocalTableManagement() {
        processor.start();
        assertTrue(processor.isRunning());

        // 添加本地表
        TableInfo table1 = new TableInfo();
        table1.setTableName("table1");
        processor.addLocalTable(table1);

        // 验证本地表
        Map<String, TableInfo> localTables = processor.getLocalTables();
        assertEquals(1, localTables.size());
        assertTrue(localTables.containsKey("table1"));

        // 移除本地表
        processor.removeLocalTable("table1");
        localTables = processor.getLocalTables();
        assertEquals(0, localTables.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testProcessRequestWhenStopped() {
        // 不启动处理器
        processor.processRequest(new Object()); // 应该抛出异常
    }

    @Test
    public void testConcurrentTableOperations() throws Exception {
        processor.start();
        assertTrue(processor.isRunning());

        int operationCount = 10;
        CountDownLatch operationLatch = new CountDownLatch(operationCount);

        // 并发执行表操作
        for (int i = 0; i < operationCount; i++) {
            final int tableId = i;
            Thread thread = new Thread(() -> {
                try {
                    TableInfo table = new TableInfo();
                    table.setTableName("table" + tableId);
                    processor.addLocalTable(table);
                    Thread.sleep(100);
                    processor.removeLocalTable("table" + tableId);
                    operationLatch.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            thread.start();
        }

        // 等待所有操作完成
        assertTrue(operationLatch.await(5, TimeUnit.SECONDS));
        assertEquals(0, processor.getLocalTables().size());
    }

    private RegionInfo createRegionInfo(String regionId) {
        RegionInfo info = new RegionInfo();
        info.setRegionId(regionId);
        info.setHost("localhost");
        info.setPort(8080);
        info.setStatus("ACTIVE");
        info.setCreateTime(System.currentTimeMillis());
        return info;
    }
} 