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
    public void testFullWorkflow() throws Exception {
        System.out.println("=== 测试完整工作流（Master -> RegionServer -> Client）===");

        // 1. 启动Master
        System.out.println("正在启动Master...");
        Thread masterThread = new Thread(() -> {
            try {
                com.mds.master.MasterMain.main(new String[0]);
            } catch (Exception e) {
                System.err.println("Master启动异常: " + e.getMessage());
            }
        });
        masterThread.setDaemon(true);
        masterThread.start();

        // 等待Master启动
        System.out.println("等待Master启动...");
        Thread.sleep(5000);

        // 2. 启动RegionServer
        System.out.println("正在启动RegionServer...");
        Thread regionThread = new Thread(() -> {
            try {
                com.mds.region.RegionServer server = new com.mds.region.RegionServer("localhost", 8100, "1");
                server.start();
            } catch (Exception e) {
                System.err.println("RegionServer启动异常: " + e.getMessage());
            }
        });
        regionThread.setDaemon(true);
        regionThread.start();

        // 等待RegionServer启动
        System.out.println("等待RegionServer启动...");
        Thread.sleep(5000);

        // 3. 启动Client并执行SQL
        System.out.println("启动Client并执行SQL...");
        Client testClient = new Client();
        testClient.start();

        try {
            // 创建测试表
            System.out.println("创建测试表...");
            Object result = testClient.executeSql(
                    "CREATE TABLE test_users (id INT PRIMARY KEY, name VARCHAR(50), age INT)",
                    null);
            System.out.println("创建表结果: " + result);

            // 插入测试数据
            System.out.println("插入测试数据...");
            result = testClient.executeSql(
                    "INSERT INTO test_users (id, name, age) VALUES (1, '测试用户', 25)",
                    null);
            System.out.println("插入结果: " + result);

            // 查询测试数据
            System.out.println("查询测试数据...");
            result = testClient.executeSql(
                    "SELECT * FROM test_users WHERE id = 1",
                    null);
            System.out.println("查询结果: " + result);

        } catch (Exception e) {
            System.err.println("SQL执行失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭Client
            testClient.stop();
            System.out.println("测试完成");
        }
    }
}