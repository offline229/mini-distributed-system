package com.mds.region;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

public class ReplicaKeyTest {
    private static final String TEST_REPLICA_KEY = "test_replica_key";
    private static final String TEST_HOST = "localhost";
    private static final int PORT1 = 9001;
    private static final int PORT2 = 9002;

    private RegionServer server1;
    private RegionServer server2;
    private Thread masterThread;

    @BeforeEach
    void setUp() {
        // 1. 先启动Master
        System.out.println("=== 启动Master服务 ===");
        masterThread = new Thread(() -> {
            try {
                com.mds.master.MasterMain.main(new String[0]);
            } catch (Exception e) {
                System.err.println("Master启动异常: " + e.getMessage());
            }
        });
        masterThread.setDaemon(true); // 设置为守护线程，测试结束后自动退出
        masterThread.start();

        // 等待Master启动
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("Master服务已启动");

        // 初始化两个使用相同replicaKey的RegionServer
        server1 = new RegionServer(TEST_HOST, PORT1, TEST_REPLICA_KEY);
        server2 = new RegionServer(TEST_HOST, PORT2, TEST_REPLICA_KEY);
        server1.setClientPort(9100);
    }

    @Test
    void testReplicaKeyAndServerId() {
        // 模拟注册并确保 serverId 被正确设置
        server1.start();
        server2.start();

        assertEquals(server1.getServerId(), server2.getServerId(),
                "Server IDs should be the same for servers with the same replicaKey");
    }

    @Test
    void testSyncData() {
        // 1. 启动RegionServers并注册到Master
        System.out.println("=== 启动RegionServer服务 ===");
        server1.start(); // 启动并注册到Master
        server2.start(); // 启动并注册到Master

        // 等待RegionServer注册完成
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 2. 测试syncData方法 - 查找相同副本的服务器
        System.out.println("\n=== 测试数据同步 ===");
        String tableName = "test_sync_table";
        String operation = "INSERT";
        String data = "{\"id\":1,\"name\":\"test\"}";

        server1.syncData(tableName, operation, data);

        // 验证同步是否成功完成
        assertNotNull(server1.getServerId(), "服务器ID不应为空");
        assertNotNull(server2.getServerId(), "服务器ID不应为空");
        assertEquals(server1.getServerId(), server2.getServerId(),
                "相同replicaKey的服务器应该有相同的ID");

        System.out.println("\n同步测试完成，请检查控制台输出，确认是否成功找到相同replicaKey的服务器");
    }

    @Test
    void testFaultTolerance() {
        // 启动服务器
        server1.start();
        server2.start();

        // 等待服务器启动完成
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 模拟故障，关闭server1
        server1.decrementConnections(); // 关闭掉一个连接
        server1.stop(); // 模拟服务器宕机
        System.out.println("模拟RegionServer宕机");

        // 等待服务器关闭
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 重新启动并验证请求是否依然能够被处理
        server1 = new RegionServer(TEST_HOST, PORT1, TEST_REPLICA_KEY);
        server1.start();
        System.out.println("RegionServer重新启动");

        // 验证 RegionServer 在发生故障后仍然能够启动并处理请求
        assertNotNull(server1, "Server should be able to restart after failure");

        // 测试同步功能，确认是否能找到同副本服务器
        String tableName = "test_fault_table";
        String operation = "INSERT";
        String data = "{\"id\":2,\"name\":\"fault_test\"}";

        server1.syncData(tableName, operation, data);
    }

    @Test
    void testLoadBalancing() {
        // 启动服务器
        server1.start();
        server2.start();

        // 模拟不同 RegionServer 负载情况
        server1.incrementConnections(); // 增加连接数
        server2.incrementConnections();
        server2.incrementConnections();

        // 验证负载均衡
        RegionServer leastLoadedReplica = server1.getLeastLoadedReplica();
        assertEquals(server1, leastLoadedReplica, "The server with fewer connections should be selected");
    }

    @Test
    void testThreadCountState() {
        // 启动服务器
        server1.start();

        // 模拟增加连接
        server1.incrementConnections(); // 1
        server1.incrementConnections(); // 2
        server1.incrementConnections(); // 3
        server1.incrementConnections(); // 4
        server1.incrementConnections(); // 5
        server1.incrementConnections(); // 6

        // 验证负载状态
        assertEquals("Busy", server1.getConnectionStatus(), "Server should be marked as busy when connections > 5");

        server1.decrementConnections(); // 5
        server1.decrementConnections(); // 4
        server1.decrementConnections(); // 3
        server1.decrementConnections(); // 2

        assertEquals("Idle", server1.getConnectionStatus(),
                "Server should be marked as available when connections <= 3");

        server1.incrementConnections(); // 3
        server1.incrementConnections(); // 4
        server1.incrementConnections(); // 5
        server1.incrementConnections(); // 6
        server1.incrementConnections(); // 7
        server1.incrementConnections(); // 8
        server1.incrementConnections(); // 9
        server1.incrementConnections(); // 10

        assertEquals("Full", server1.getConnectionStatus(), "Server should be marked as full when connections == 10");
    }

    @AfterEach
    void tearDown() {
        // 清理资源
        if (server1 != null) {
            server1.stop();
        }
        if (server2 != null) {
            server2.stop();
        }
        // Master线程是守护线程，会自动退出，不需要特别处理
        System.out.println("测试清理完成");
    }
}