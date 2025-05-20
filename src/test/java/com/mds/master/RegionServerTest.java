package com.mds.master;

import com.mds.common.RegionServerInfo;
import com.mds.common.RegionServerInfo.HostPortStatus;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class RegionServerTest {
    private TestingServer zkServer;
    private CuratorFramework zkClient;
    private RegionWatcher regionWatcher;
    private ObjectMapper objectMapper;
    private static final String REGIONS_PATH = "/mds/regions-meta";
    private static final int WAIT_TIME = 2000;

    @Before
    public void setUp() throws Exception {
        // 启动内嵌的ZooKeeper服务器
        zkServer = new TestingServer(2181, true);

        // 创建ZK客户端
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkServer.getConnectString())
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("mds")  // 添加命名空间
                .build();
        zkClient.start();

        // 等待连接建立
        if (!zkClient.blockUntilConnected(10, TimeUnit.SECONDS)) {
            throw new RuntimeException("连接ZooKeeper超时");
        }

        // 初始化ObjectMapper
        objectMapper = new ObjectMapper();

        // 初始化RegionWatcher
        regionWatcher = new RegionWatcher(zkClient);
        regionWatcher.startWatching();
        
        // 确保基础路径存在
        ensureBasePath();
    }

    private void ensureBasePath() throws Exception {
        if (zkClient.checkExists().forPath(REGIONS_PATH) == null) {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath(REGIONS_PATH);
        }
    }

    private RegionServerInfo createTestRegionServer(String id, String host, int port, String replicaKey) {
        // 创建RegionServer基本信息
        RegionServerInfo region = new RegionServerInfo();
        region.setRegionserverID(id);
        region.setReplicaKey(replicaKey);
        
        // 创建状态信息列表
        List<HostPortStatus> statusList = new ArrayList<>();
        HostPortStatus status = new HostPortStatus();
        status.setHost(host);
        status.setPort(port);
        status.setConnections(0);
        status.setStatus("healthy");
        statusList.add(status);
        
        // 设置状态列表
        region.setHostsPortsStatusList(statusList);
        
        return region;
    }

    @Test
    public void testBasicRegisterAndUnregister() throws Exception {
        // 创建测试用RegionServer
        RegionServerInfo testRegion = createTestRegionServer(
            "test-basic-region", "localhost", 9000, "replica-1");

        // 注册RegionServer
        String path = REGIONS_PATH + "/" + testRegion.getRegionserverID();
        byte[] data = objectMapper.writeValueAsBytes(testRegion);
        zkClient.create()
                .creatingParentsIfNeeded()
                .forPath(path, data);

        // 等待注册完成
        Thread.sleep(WAIT_TIME);
        
        // 验证注册
        Map<String, RegionServerInfo> onlineRegions = regionWatcher.getOnlineRegions();
        assertNotNull("在线Region列表不应为null", onlineRegions);
        
        RegionServerInfo registered = onlineRegions.get("test-basic-region");
        assertNotNull("应该能找到注册的Region", registered);
        
        // 验证状态信息
        List<HostPortStatus> statusList = registered.getHostsPortsStatusList();
        assertNotNull("状态列表不应为null", statusList);
        assertEquals("应该只有一个状态项", 1, statusList.size());
        
        HostPortStatus status = statusList.get(0);
        assertEquals("应该是localhost", "localhost", status.getHost());
        assertEquals("端口应该是9000", 9000, status.getPort());
        assertEquals("状态应该是healthy", "healthy", status.getStatus());
        
        // 注销测试
        zkClient.delete().forPath(path);
        Thread.sleep(WAIT_TIME);
        
        onlineRegions = regionWatcher.getOnlineRegions();
        assertFalse("Region应该已被移除", 
                onlineRegions.containsKey("test-basic-region"));
    }

    @Test
    public void testMultipleRegions() throws Exception {
        // 创建多个RegionServer
        Map<String, RegionServerInfo> testRegions = new HashMap<>();
        for (int i = 1; i <= 3; i++) {
            String regionId = "test-multi-region-" + i;
            RegionServerInfo region = createTestRegionServer(
                regionId, "localhost", 9000 + i, "replica-" + i);
            testRegions.put(regionId, region);
            
            // 注册到ZK
            String path = REGIONS_PATH + "/" + regionId;
            byte[] data = objectMapper.writeValueAsBytes(region);
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, data);
        }

        // 等待所有注册完成
        Thread.sleep(WAIT_TIME);
        
        // 验证注册情况
        Map<String, RegionServerInfo> onlineRegions = regionWatcher.getOnlineRegions();
        assertEquals("应该有3个Region在线", 3, onlineRegions.size());
        
        // 验证每个Region的信息
        testRegions.forEach((regionId, expectedRegion) -> {
            RegionServerInfo actualRegion = onlineRegions.get(regionId);
            assertNotNull("应该能找到Region: " + regionId, actualRegion);
            assertEquals("ReplicaKey应该匹配", 
                    expectedRegion.getReplicaKey(), 
                    actualRegion.getReplicaKey());
            
            // 验证状态信息
            List<HostPortStatus> expectedStatus = expectedRegion.getHostsPortsStatusList();
            List<HostPortStatus> actualStatus = actualRegion.getHostsPortsStatusList();
            
            assertEquals("状态列表大小应该相同", 
                    expectedStatus.size(), 
                    actualStatus.size());
            
            // 验证第一个状态项
            HostPortStatus expected = expectedStatus.get(0);
            HostPortStatus actual = actualStatus.get(0);
            assertEquals("Host应该匹配", expected.getHost(), actual.getHost());
            assertEquals("Port应该匹配", expected.getPort(), actual.getPort());
            assertEquals("Status应该匹配", expected.getStatus(), actual.getStatus());
        });
    }

    @Test
    public void testHeartbeatTimeoutDetection() throws Exception {
        // 创建测试RegionServer
        String regionId = "test-heartbeat-region";
        RegionServerInfo region = createTestRegionServer(regionId, "localhost", 9100, "replica-heartbeat");
        // 设置心跳时间为很久以前，模拟超时
        region.getHostsPortsStatusList().get(0).setLastHeartbeatTime(System.currentTimeMillis() - 60_000);

        // 注册到ZK
        String path = REGIONS_PATH + "/" + regionId;
        byte[] data = objectMapper.writeValueAsBytes(region);
        zkClient.create().creatingParentsIfNeeded().forPath(path, data);

        Thread.sleep(WAIT_TIME);

        // RegionWatcher应能检测到该Region
        assertTrue(regionWatcher.getOnlineRegions().containsKey(regionId));

        // 手动触发心跳超时检测
        regionWatcher.checkHeartbeatTimeout();

        // 检查超时后Region是否被移除
        Map<String, RegionServerInfo> onlineRegions = regionWatcher.getOnlineRegions();
        assertFalse("超时后Region应被移除", onlineRegions.containsKey(regionId));
    }

    @Test
    public void testHandleZKExceptionGracefully() throws Exception {
        // 模拟ZK异常：关闭zkClient后再调用RegionWatcher方法
        zkClient.close();

        try {
            regionWatcher.getOnlineRegions();
            // 如果没有抛异常，说明处理正常
        } catch (Exception e) {
            fail("RegionWatcher应能优雅处理ZK异常，不应抛出未捕获异常");
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            // 清理所有测试节点
            if (zkClient.checkExists().forPath(REGIONS_PATH) != null) {
                zkClient.delete().deletingChildrenIfNeeded().forPath(REGIONS_PATH);
            }
        } catch (Exception e) {
            System.err.println("清理测试节点失败: " + e.getMessage());
        }

        // 关闭资源
        if (regionWatcher != null) {
            regionWatcher.stop();
        }
        if (zkClient != null) {
            zkClient.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }
}