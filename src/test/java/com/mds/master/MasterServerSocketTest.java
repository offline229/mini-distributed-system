package com.mds.master;

import com.mds.common.RegionServerInfo;
import com.mds.master.self.MetaManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class MasterServerSocketTest {

    private MasterServer masterServer;
    private MetaManager metaManager;
    private ZKSyncManager zkSyncManager;
    private RegionWatcher regionWatcher;
    private MasterDispatcher masterDispatcher;
    private int port;

    // 动态分配可用端口
    private int getFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    @BeforeEach
    public void startServer() throws Exception {
        port = getFreePort();
        metaManager = mock(MetaManager.class);
        zkSyncManager = mock(ZKSyncManager.class);
        regionWatcher = mock(RegionWatcher.class);
        masterDispatcher = mock(MasterDispatcher.class);
        masterServer = new MasterServer(port, metaManager, zkSyncManager, regionWatcher, masterDispatcher);
        new Thread(() -> {
            try {
                masterServer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        Thread.sleep(500); // 等待服务启动
    }

    @AfterEach
    public void stopServer() throws Exception {
        masterServer.stop();
        Thread.sleep(500); // 等待端口释放
    }

    @Test
    public void testHeartbeatSocketCommunication() throws Exception {
        String heartbeatJson = "{\"type\":\"heartbeat\",\"regionserverID\":\"region-1\",\"replicaKey\":\"replica-1\"}";

        try (Socket socket = new Socket("localhost", port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println(heartbeatJson);
            String response = in.readLine();
            assertNotNull(response);
            assertTrue(response.contains("success") || response.contains("ok"));
        }
    }

    @Test
    public void testRegisterSocketCommunication() throws Exception {
        String registerJson = "{\"type\":\"register\",\"regionserverID\":\"region-2\",\"replicaKey\":\"replica-2\",\"host\":\"127.0.0.1\",\"port\":9200}";

        try (Socket socket = new Socket("localhost", port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println(registerJson);
            String response = in.readLine();
            assertNotNull(response);
            assertTrue(response.contains("success") || response.contains("ok") || response.contains("registered"));
        }
    }

    @Test
    public void testSQLSocketCommunication() throws Exception {
        String sqlJson = "{\"type\":\"sql\",\"sql\":\"SELECT * FROM test_table\"}";

        when(masterDispatcher.dispatch(anyString())).thenReturn(
                Map.of(
                        "type", "DML_REDIRECT",
                        "regionId", "region-2",
                        "host", "127.0.0.2",
                        "port", 9201
                )
        );

        try (Socket socket = new Socket("localhost", port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println(sqlJson);
            String response = in.readLine();
            assertNotNull(response);
            assertTrue(response.contains("region-2") && response.contains("127.0.0.2"));
        }
    }

    @Test
    public void testMalformedJsonInput() throws Exception {
        String badJson = "{this is not valid json!";

        try (Socket socket = new Socket("localhost", port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println(badJson);
            String response = in.readLine();
            assertNotNull(response);
            assertTrue(response.toLowerCase().contains("error") || response.toLowerCase().contains("fail"));
        }
    }

    @Test
    public void testUnknownRequestType() throws Exception {
        String unknownJson = "{\"type\":\"unknown_type\",\"foo\":\"bar\"}";

        try (Socket socket = new Socket("localhost", port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println(unknownJson);
            String response = in.readLine();
            assertNotNull(response);
            assertTrue(response.toLowerCase().contains("error") || response.toLowerCase().contains("unknown"));
        }
    }

    @Test
    public void testNetworkAbruptDisconnect() throws Exception {
        try (Socket socket = new Socket("localhost", port)) {
            // 不发送任何数据，直接关闭
        }
        assertTrue(true);
    }

    @Test
    public void testServerStopAndNoLongerAcceptsConnections() throws Exception {
        masterServer.stop();
        Thread.sleep(500); // 等待服务完全关闭

        Exception exception = null;
        try (Socket socket = new Socket("localhost", port)) {
            // 不应该连得上
        } catch (IOException e) {
            exception = e;
        }
        assertNotNull(exception, "服务关闭后应无法连接");

        // 重新启动，保证后续测试不受影响
        new Thread(() -> {
            try {
                masterServer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        Thread.sleep(1000);
    }
}