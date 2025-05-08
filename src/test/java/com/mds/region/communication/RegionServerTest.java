package com.mds.region.communication;

import com.mds.common.model.RegionInfo;
import com.mds.common.util.ZookeeperUtil;
import com.mds.common.config.SystemConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RegionServerTest {
    private static final String TEST_REGION_ID = "test-region-1";
    private static final String TEST_HOST = "localhost";
    private static final int TEST_PORT = 8080;
    private static final String ZK_CONNECT_STRING = "localhost:2181";
    private static final int ZK_TIMEOUT = 5000;

    private ZookeeperUtil zkUtil;
    private RegionServer server;
    private CountDownLatch clientLatch;

    @Before
    public void setUp() throws Exception {
        zkUtil = new ZookeeperUtil();
        zkUtil.connect(ZK_CONNECT_STRING, ZK_TIMEOUT);
        server = new RegionServer(TEST_REGION_ID, TEST_HOST, TEST_PORT, zkUtil);
        clientLatch = new CountDownLatch(1);
    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (zkUtil != null) {
            zkUtil.close();
        }
    }

    @Test
    public void testServerStartAndStop() throws Exception {
        // 启动服务器
        server.start();
        assertTrue(server.isRunning());

        // 验证ZooKeeper注册
        String regionPath = SystemConfig.ZK_REGION_PATH + "/" + TEST_REGION_ID;
        assertTrue(zkUtil.exists(regionPath));

        // 停止服务器
        server.stop();
        assertFalse(server.isRunning());
        assertFalse(zkUtil.exists(regionPath));
    }

    @Test
    public void testClientConnection() throws Exception {
        // 启动服务器
        server.start();
        assertTrue(server.isRunning());

        // 创建测试客户端
        Thread clientThread = new Thread(() -> {
            try {
                Socket clientSocket = new Socket(TEST_HOST, TEST_PORT);
                // 等待一段时间后关闭连接
                Thread.sleep(100);
                clientSocket.close();
                clientLatch.countDown();
            } catch (Exception e) {
                fail("Client connection failed: " + e.getMessage());
            }
        });
        clientThread.start();

        // 等待客户端连接完成
        assertTrue(clientLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleClientConnections() throws Exception {
        // 启动服务器
        server.start();
        assertTrue(server.isRunning());

        int clientCount = 5;
        CountDownLatch multiClientLatch = new CountDownLatch(clientCount);

        // 创建多个测试客户端
        for (int i = 0; i < clientCount; i++) {
            Thread clientThread = new Thread(() -> {
                try {
                    Socket clientSocket = new Socket(TEST_HOST, TEST_PORT);
                    Thread.sleep(100);
                    clientSocket.close();
                    multiClientLatch.countDown();
                } catch (Exception e) {
                    fail("Client connection failed: " + e.getMessage());
                }
            });
            clientThread.start();
        }

        // 等待所有客户端连接完成
        assertTrue(multiClientLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testServerRestart() throws Exception {
        // 第一次启动
        server.start();
        assertTrue(server.isRunning());
        String regionPath = SystemConfig.ZK_REGION_PATH + "/" + TEST_REGION_ID;
        assertTrue(zkUtil.exists(regionPath));

        // 停止
        server.stop();
        assertFalse(server.isRunning());
        assertFalse(zkUtil.exists(regionPath));

        // 第二次启动
        server.start();
        assertTrue(server.isRunning());
        assertTrue(zkUtil.exists(regionPath));
    }

    @Test(expected = IllegalStateException.class)
    public void testDoubleStart() throws Exception {
        server.start();
        server.start(); // 应该抛出异常
    }
} 