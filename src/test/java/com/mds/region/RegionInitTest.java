package com.mds.region;

import com.mds.common.model.RegionInfo;
import com.mds.common.util.ZookeeperUtil;
import com.mds.common.config.SystemConfig;
import com.mds.region.communication.MasterClient;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RegionInitTest {
    private static final String TEST_REGION_ID = "test-region-1";
    private static final String TEST_HOST = "localhost";
    private static final int TEST_PORT = 8080;
    private static final int ZK_PORT = 2181;
    private static final String ZK_HOST = "localhost";
    private static final int ZK_TIMEOUT = 5000;

    private ZookeeperUtil zkUtil;
    private ServerSocket serverSocket;
    private CountDownLatch serverLatch;
    private Thread serverThread;

    @Before
    public void setUp() throws Exception {
        // 初始化ZooKeeper连接
        zkUtil = new ZookeeperUtil();
        zkUtil.connect(ZK_HOST + ":" + ZK_PORT, ZK_TIMEOUT);

        // 启动测试服务器
        serverSocket = new ServerSocket(TEST_PORT);
        serverLatch = new CountDownLatch(1);
        startTestServer();
    }

    private void startTestServer() {
        serverThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Socket clientSocket = serverSocket.accept();
                    // 处理客户端连接
                    handleClientConnection(clientSocket);
                }
            } catch (IOException e) {
                if (!Thread.currentThread().isInterrupted()) {
                    e.printStackTrace();
                }
            }
        });
        serverThread.start();
    }

    private void handleClientConnection(Socket clientSocket) {
        // 这里可以添加具体的客户端连接处理逻辑
        try {
            // 模拟处理客户端请求
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @After
    public void tearDown() throws Exception {
        // 关闭测试服务器
        if (serverThread != null) {
            serverThread.interrupt();
        }
        if (serverSocket != null) {
            serverSocket.close();
        }
        // 关闭ZooKeeper连接
        if (zkUtil != null) {
            zkUtil.close();
        }
    }

    @Test
    public void testZooKeeperConnection() throws Exception {
        // 测试ZooKeeper连接
        assertTrue(zkUtil.isConnected());
        
        // 测试创建节点
        String testPath = "/test/node";
        zkUtil.createPath(testPath, org.apache.zookeeper.CreateMode.PERSISTENT);
        assertTrue(zkUtil.exists(testPath));
        
        // 测试删除节点
        zkUtil.delete(testPath);
        assertFalse(zkUtil.exists(testPath));
    }

    @Test
    public void testRegionRegistration() throws Exception {
        // 创建Region信息
        RegionInfo regionInfo = new RegionInfo();
        regionInfo.setRegionId(TEST_REGION_ID);
        regionInfo.setHost(TEST_HOST);
        regionInfo.setPort(TEST_PORT);
        regionInfo.setStatus("ACTIVE");
        regionInfo.setCreateTime(System.currentTimeMillis());

        // 注册Region节点
        String regionPath = SystemConfig.ZK_REGION_PATH + "/" + TEST_REGION_ID;
        zkUtil.createPath(regionPath, org.apache.zookeeper.CreateMode.EPHEMERAL);
        zkUtil.setData(regionPath, regionInfo.toString().getBytes());

        // 验证Region节点
        assertTrue(zkUtil.exists(regionPath));
        byte[] data = zkUtil.getData(regionPath);
        assertNotNull(data);
        assertTrue(new String(data).contains(TEST_REGION_ID));

        // 清理
        zkUtil.delete(regionPath);
    }

    @Test
    public void testMasterClientConnection() throws Exception {
        // 创建MasterClient
        MasterClient masterClient = new MasterClient(TEST_HOST, TEST_PORT, TEST_REGION_ID);
        
        // 测试连接
        assertTrue(masterClient.isConnected());
        
        // 测试心跳
        assertTrue(masterClient.sendHeartbeat());
        
        // 测试状态报告
        RegionInfo status = new RegionInfo();
        status.setRegionId(TEST_REGION_ID);
        status.setStatus("ACTIVE");
        status.setLastHeartbeat(System.currentTimeMillis());
        assertTrue(masterClient.reportStatus(status));
        
        // 关闭连接
        masterClient.close();
    }

    @Test
    public void testClusterManagement() throws Exception {
        // 创建多个Region节点
        String[] regionIds = {"region-1", "region-2", "region-3"};
        for (String regionId : regionIds) {
            String regionPath = SystemConfig.ZK_REGION_PATH + "/" + regionId;
            RegionInfo regionInfo = new RegionInfo();
            regionInfo.setRegionId(regionId);
            regionInfo.setHost(TEST_HOST);
            regionInfo.setPort(TEST_PORT);
            regionInfo.setStatus("ACTIVE");
            regionInfo.setCreateTime(System.currentTimeMillis());
            
            zkUtil.createPath(regionPath, org.apache.zookeeper.CreateMode.EPHEMERAL);
            zkUtil.setData(regionPath, regionInfo.toString().getBytes());
        }

        // 验证所有Region节点
        for (String regionId : regionIds) {
            String regionPath = SystemConfig.ZK_REGION_PATH + "/" + regionId;
            assertTrue(zkUtil.exists(regionPath));
        }

        // 获取所有Region节点
        String[] children = zkUtil.getChildren(SystemConfig.ZK_REGION_PATH);
        assertEquals(regionIds.length, children.length);

        // 清理
        for (String regionId : regionIds) {
            String regionPath = SystemConfig.ZK_REGION_PATH + "/" + regionId;
            zkUtil.delete(regionPath);
        }
    }

    @Test
    public void testRegionFailureDetection() throws Exception {
        // 创建Region节点
        String regionPath = SystemConfig.ZK_REGION_PATH + "/" + TEST_REGION_ID;
        RegionInfo regionInfo = new RegionInfo();
        regionInfo.setRegionId(TEST_REGION_ID);
        regionInfo.setHost(TEST_HOST);
        regionInfo.setPort(TEST_PORT);
        regionInfo.setStatus("ACTIVE");
        regionInfo.setCreateTime(System.currentTimeMillis());
        
        zkUtil.createPath(regionPath, org.apache.zookeeper.CreateMode.EPHEMERAL);
        zkUtil.setData(regionPath, regionInfo.toString().getBytes());

        // 验证节点存在
        assertTrue(zkUtil.exists(regionPath));

        // 模拟Region故障（删除节点）
        zkUtil.delete(regionPath);

        // 验证节点已删除
        assertFalse(zkUtil.exists(regionPath));
    }

    @Test
    public void testRegionRecovery() throws Exception {
        // 创建Region节点
        String regionPath = SystemConfig.ZK_REGION_PATH + "/" + TEST_REGION_ID;
        RegionInfo regionInfo = new RegionInfo();
        regionInfo.setRegionId(TEST_REGION_ID);
        regionInfo.setHost(TEST_HOST);
        regionInfo.setPort(TEST_PORT);
        regionInfo.setStatus("ACTIVE");
        regionInfo.setCreateTime(System.currentTimeMillis());
        
        zkUtil.createPath(regionPath, org.apache.zookeeper.CreateMode.EPHEMERAL);
        zkUtil.setData(regionPath, regionInfo.toString().getBytes());

        // 模拟Region故障
        zkUtil.delete(regionPath);

        // 模拟Region恢复
        zkUtil.createPath(regionPath, org.apache.zookeeper.CreateMode.EPHEMERAL);
        zkUtil.setData(regionPath, regionInfo.toString().getBytes());

        // 验证节点已恢复
        assertTrue(zkUtil.exists(regionPath));
        byte[] data = zkUtil.getData(regionPath);
        assertNotNull(data);
        assertTrue(new String(data).contains(TEST_REGION_ID));

        // 清理
        zkUtil.delete(regionPath);
    }
} 