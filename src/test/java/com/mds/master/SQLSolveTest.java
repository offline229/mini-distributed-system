package com.mds.master;

import com.mds.common.RegionServerInfo;
import com.mds.common.RegionServerInfo.HostPortStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SQLSolveTest {

    private MasterDispatcher dispatcher;
    private RegionWatcher regionWatcher;

    @BeforeEach
public void setUp() {
    // Mock RegionWatcher
    regionWatcher = mock(RegionWatcher.class);

    // Initialize MasterDispatcher with mocked RegionWatcher
    dispatcher = new MasterDispatcher(regionWatcher);

    // 启动 dispatcher
    dispatcher.start();
}

    @Test
    public void testHandleDMLRequest() {
        // Mock两个RegionServer，region-2连接数更少
        RegionServerInfo region1 = new RegionServerInfo("region-1", List.of(
            new HostPortStatus("127.0.0.1", 8081, "active", 5, System.currentTimeMillis())
        ), "replica-1", System.currentTimeMillis());

        RegionServerInfo region2 = new RegionServerInfo("region-2", List.of(
            new HostPortStatus("127.0.0.2", 8082, "active", 2, System.currentTimeMillis())
        ), "replica-2", System.currentTimeMillis());

        when(regionWatcher.getOnlineRegions()).thenReturn(Map.of(
            "region-1", region1,
            "region-2", region2
        ));

        // 分发DML请求
        Map<String, Object> response = dispatcher.dispatch("INSERT INTO table VALUES (1)");

        // 验证分发到连接数最少的region-2
        assertEquals(MasterDispatcher.RESPONSE_TYPE_DML_REDIRECT, response.get("type"));
        assertEquals("region-2", response.get("regionId"));
        assertEquals("127.0.0.2", response.get("host"));
        assertEquals(8082, response.get("port"));
    }

    @Test
    public void testHandleDDLRequest() {
        // Mock两个RegionServer
        RegionServerInfo region1 = new RegionServerInfo("region-1", List.of(
            new HostPortStatus("127.0.0.1", 8081, "active", 5, System.currentTimeMillis())
        ), "replica-1", System.currentTimeMillis());

        RegionServerInfo region2 = new RegionServerInfo("region-2", List.of(
            new HostPortStatus("127.0.0.2", 8082, "active", 2, System.currentTimeMillis())
        ), "replica-2", System.currentTimeMillis());

        when(regionWatcher.getOnlineRegions()).thenReturn(Map.of(
            "region-1", region1,
            "region-2", region2
        ));

        // 分发DDL请求
        Map<String, Object> response = dispatcher.dispatch("CREATE TABLE test (id INT)");

        // 验证只返回一个region
        assertEquals(MasterDispatcher.RESPONSE_TYPE_DDL_RESULT, response.get("type"));
        assertEquals("region-2", response.get("regionId"));
        assertEquals("127.0.0.2", response.get("host"));
        assertEquals(8082, response.get("port"));
    }

    @Test
    public void testHandleDMLRequestNoAvailableRegion() {
        // 没有可用RegionServer
        when(regionWatcher.getOnlineRegions()).thenReturn(Map.of());

        Map<String, Object> response = dispatcher.dispatch("INSERT INTO table VALUES (1)");

        assertEquals(MasterDispatcher.RESPONSE_TYPE_ERROR, response.get("type"));
        assertEquals("No available RegionServer", response.get("message"));
    }

    @Test
    public void testHandleDDLRequestNoAvailableRegion() {
        // 没有可用RegionServer
        when(regionWatcher.getOnlineRegions()).thenReturn(Map.of());

        Map<String, Object> response = dispatcher.dispatch("CREATE TABLE test (id INT)");

        assertEquals(MasterDispatcher.RESPONSE_TYPE_ERROR, response.get("type"));
        assertEquals("No available RegionServer", response.get("message"));
    }
}