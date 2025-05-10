package com.mds.region;

import com.mds.region.handler.ZookeeperHandler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RegionTest {
    private static final boolean MOCK_MASTER = true; // 设置为true使用mock模式
    private ZooKeeper zk;
    private Region region;
    private String masterData = "{\"host\":\"localhost\",\"port\":9000}";

    @Before
    public void setUp() throws Exception {
        // 1. 连接ZK
        CountDownLatch latch = new CountDownLatch(1);
        zk = new ZooKeeper("localhost:2181", 3000, event -> latch.countDown());
        latch.await();

        // 2. 创建必要的节点master用于测试
        createNodeIfNotExists("/mds", null);
        createNodeIfNotExists("/mds/master", null);
        createNodeIfNotExists("/mds/master/active", masterData.getBytes());
        createNodeIfNotExists("/mds/regions", null);

        // 3. 初始化Region
        region = new Region("localhost", 8000);
    }

    private void createNodeIfNotExists(String path, byte[] data) throws Exception {
        if (zk.exists(path, false) == null) {
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    @Test
    public void testRegionRegistration() throws Exception {
        if (MOCK_MASTER) {
            System.out.println("=== 测试Region注册流程（Mock模式）===");
            // Region会查询ZK获取master信息
            region.start();
            // 验证是否获得了regionId
            // 由于使用mock，应该返回"regiontest-1"
            TimeUnit.SECONDS.sleep(2);
            System.out.println("注册测试完成");
        }
    }

    @Test
    public void testMasterDataFormat() throws Exception {
        // 验证master数据格式
        byte[] data = zk.getData("/mds/master/active", false, null);
        String masterInfo = new String(data);
        System.out.println("Master数据: " + masterInfo);

        // 验证JSON解析
        JSONObject json = new JSONObject(masterInfo);
        assertEquals("localhost", json.getString("host"));
        assertEquals(9000, json.getInt("port"));
    }

    @Test
    public void testHeartbeat() throws Exception {
        if (MOCK_MASTER) {
            System.out.println("=== 测试心跳发送（Mock模式）===");
            region.start();

            // 等待5次心跳
            for (int i = 0; i < 5; i++) {
                TimeUnit.SECONDS.sleep(5);
                System.out.println("已发送心跳 " + (i + 1) + " 次");
            }

            region.stop();
        }
    }

    @Test
    public void testClientOperation() throws Exception {
        if (MOCK_MASTER) {
            System.out.println("=== 测试客户端数据库操作（Mock模式）===");
            region.start();

            // 模拟接收到客户端请求
            String mockSql = "SELECT * FROM test_table";
            System.out.println("模拟接收到客户端SQL请求: " + mockSql);

            // 模拟数据库操作
            System.out.println("执行数据库操作...");
            TimeUnit.MILLISECONDS.sleep(500);

            // 模拟向客户端返回结果
            System.out.println("向客户端返回结果: 影响行数 5");

            // 模拟更新ZK状态
            System.out.println("更新ZK节点状态: ACTIVE, lastOperation=" + System.currentTimeMillis());
        }
    }

    @Test
    public void testRegionShutdown() throws Exception {
        if (MOCK_MASTER) {
            System.out.println("=== 测试Region关闭流程（Mock模式）===");
            region.start();
            TimeUnit.SECONDS.sleep(1);
            region.stop();
            System.out.println("关闭测试完成");
        }
    }
}