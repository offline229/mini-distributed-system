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
    }

    @Test
    void testReplicaRegistration() {
        // 启动两个服务器
        server1.start();
        server2.start();

   }

   @Test
   void testReplicaKeyAndServerId() {
       // 模拟注册并确保 serverId 被正确设置
       server1.start();
       server2.start();
       
       assertEquals(server1.getServerId(), server2.getServerId(), "Server IDs should be the same for servers with the same replicaKey");
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