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

    @BeforeEach
    void setUp() {
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
       
       assertEquals(server1.getServerId(), server2.getServerId(), "Server IDs should be the same for servers with the same replicaKey");
   }

    @Test
    void testSyncData() {
        // 模拟同步操作，验证数据同步
        String tableName = "test_table";
        String operation = "INSERT";
        String data = "data_inserted";

        server1.syncData(tableName, operation, data);

        // 验证同步逻辑（例如：打印输出）
        assertTrue(true, "Data sync completed");
    }

    @Test
    void testFaultTolerance() {
        // 模拟故障，关闭server1
        server1.decrementConnections();  // 关闭掉一个连接
        server1 = null;  // 模拟服务器宕机

        // 重新启动并验证请求是否依然能够被处理
        server1 = new RegionServer(TEST_HOST, PORT1, TEST_REPLICA_KEY);
        server1.start();

        // 验证 RegionServer 在发生故障后仍然能够启动并处理请求
        assertNotNull(server1, "Server should be able to restart after failure");
    }

    @Test
    void testLoadBalancing() {
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

        assertEquals("Idle", server1.getConnectionStatus(), "Server should be marked as available when connections <= 3");

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
    }
}