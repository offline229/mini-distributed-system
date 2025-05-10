package com.mds.client;

import org.junit.Before;
import org.junit.Test;
import org.apache.zookeeper.ZooKeeper;
import java.util.concurrent.CountDownLatch;

public class ClientTest {
    private static final boolean MOCK_MODE = true;
    private ZooKeeper zk;
    private Client client;

    @Before
    public void setUp() throws Exception {
        // 1. 连接ZK
        CountDownLatch latch = new CountDownLatch(1);
        zk = new ZooKeeper("localhost:2181", 3000, event -> latch.countDown());
        latch.await();

        // 2. 初始化Client
        client = new Client();
    }

    @Test
    public void testSqlExecution() throws Exception {
        if (MOCK_MODE) {
            System.out.println("=== 测试SQL执行（Mock模式）===");
            client.start();

            // 测试查询
            Object result = client.executeSql(
                    "SELECT * FROM users WHERE id = ?",
                    new Object[] { 1 });
            System.out.println("查询结果: " + result);

            // 测试插入
            result = client.executeSql(
                    "INSERT INTO users(name,age) VALUES(?,?)",
                    new Object[] { "张三", 25 });
            System.out.println("插入结果: " + result);

            client.stop();
        }
    }
}