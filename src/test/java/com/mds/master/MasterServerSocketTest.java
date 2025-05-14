package com.mds.master;

import com.mds.common.RegionServerInfo;
import com.mds.master.self.MetaManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.Socket;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class MasterServerSocketTest {

    private static MasterServer masterServer;
    private static MetaManager metaManager;
    private static ZKSyncManager zkSyncManager;
    private static RegionWatcher regionWatcher;
    private static MasterDispatcher masterDispatcher;
    private static final int PORT = 9100;

    @BeforeAll
    public static void startServer() throws Exception {
        metaManager = mock(MetaManager.class);
        zkSyncManager = mock(ZKSyncManager.class);
        regionWatcher = mock(RegionWatcher.class);
        masterDispatcher = mock(MasterDispatcher.class);
        masterServer = new MasterServer(PORT, metaManager, zkSyncManager, regionWatcher, masterDispatcher);
        // 启动MasterServer（假设有start方法，且为非阻塞）
        new Thread(() -> {
            try {
                masterServer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        // 等待服务启动
        Thread.sleep(1000);
    }

    @AfterAll
    public static void stopServer() throws Exception {
        // 假设有stop方法
        masterServer.stop();
    }

    @Test
    public void testHeartbeatSocketCommunication() throws Exception {
        // 构造心跳请求内容（假设协议为JSON字符串）
        String heartbeatJson = "{\"type\":\"heartbeat\",\"regionserverID\":\"region-1\",\"replicaKey\":\"replica-1\"}";

        try (Socket socket = new Socket("localhost", PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 发送心跳
            out.println(heartbeatJson);

            // 读取响应
            String response = in.readLine();

            // 断言响应内容（根据你的协议设计调整）
            assertNotNull(response);
            assertTrue(response.contains("success") || response.contains("ok"));
        }
    }

    @Test
    public void testRegisterSocketCommunication() throws Exception {
        // 构造注册请求内容（假设协议为JSON字符串）
        String registerJson = "{\"type\":\"register\",\"regionserverID\":\"region-2\",\"replicaKey\":\"replica-2\",\"host\":\"127.0.0.1\",\"port\":9200}";

        try (Socket socket = new Socket("localhost", PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 发送注册请求
            out.println(registerJson);

            // 读取响应
            String response = in.readLine();

            // 断言响应内容（根据你的协议设计调整）
            assertNotNull(response);
            assertTrue(response.contains("success") || response.contains("ok") || response.contains("registered"));
        }
    }

    @Test
    public void testSQLSocketCommunication() throws Exception {
        // 构造SQL请求内容（假设协议为JSON字符串）
        String sqlJson = "{\"type\":\"sql\",\"sql\":\"SELECT * FROM test_table\"}";

        // 假设MasterDispatcher会返回一个region的host和port
        when(masterDispatcher.dispatch(anyString())).thenReturn(
                Map.of(
                        "type", "DML_REDIRECT",
                        "regionId", "region-2",
                        "host", "127.0.0.2",
                        "port", 9201
                )
        );

        try (Socket socket = new Socket("localhost", PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 发送SQL请求
            out.println(sqlJson);

            // 读取响应
            String response = in.readLine();

            // 断言响应内容（根据你的协议设计调整）
            assertNotNull(response);
            assertTrue(response.contains("region-2") && response.contains("127.0.0.2"));
        }
    }
}