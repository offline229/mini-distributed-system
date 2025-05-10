package com.mds.integration;

import com.mds.client.Client;
import com.mds.region.Region;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class DirectCommunicationTest {
    private Region region;
    private Client client;
    private static final String HOST = "localhost";
    private static final int PORT = 8000;

    @Before
    public void setUp() throws Exception {
        // 1. 启动Region
        region = new Region(HOST, PORT);
        region.start();
        Thread.sleep(1000); // 等待Region完全启动

        // 2. 启动Client
        client = new Client();
        client.start();
    }

    @Test
    public void testDirectQuery() throws Exception {
        System.out.println("=== 测试直接查询操作 ===");

        // 执行查询
        Object result = client.executeSql(
                "SELECT * FROM test_table WHERE id = ?",
                new Object[] { 2 });

        assertNotNull("查询结果不应为空", result);
        System.out.println("查询结果: " + result);
    }

    @Test
    public void testDirectInsert() throws Exception {
        System.out.println("=== 测试直接插入操作 ===");

        // 执行插入
        Object result = client.executeSql(
                "INSERT INTO test_table (name, age) VALUES ('John', 20);",
                new Object[] { "测试用户", 25 });

        assertNotNull("插入结果不应为空", result);
        System.out.println("插入影响行数: " + result);
    }

    @After
    public void tearDown() throws Exception {
        // 关闭资源
        if (client != null) {
            client.stop();
        }
        if (region != null) {
            region.stop();
        }
        Thread.sleep(1000); // 等待完全关闭
    }
}