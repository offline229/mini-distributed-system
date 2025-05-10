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
        // 3. 确保开始测试前表不存在
        try {
            client.executeSql("DROP TABLE IF EXISTS test_table", null);
            Thread.sleep(500); // 等待表删除完成
        } catch (Exception e) {
            // 忽略表不存在的错误
        }
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
        // 方式1：使用参数化查询（推荐）
        Object result = client.executeSql(
                "INSERT INTO test_table (name, age) VALUES (?, ?)",
                new Object[] { "测试用户", 25 });

        assertNotNull("插入结果不应为空", result);
        System.out.println("插入影响行数: " + result);
    }

    @Test
    public void testDatabaseOperations() throws Exception {
        System.out.println("=== 开始测试数据库操作 ===");

        try {
            // 1. 创建测试表
            System.out.println("\n1. 创建表");
            Object result = client.executeSql(
                    "CREATE TABLE IF NOT EXISTS test_table (" +
                            "id INT AUTO_INCREMENT, " +
                            "name VARCHAR(50), " +
                            "age INT, " +
                            "PRIMARY KEY (id)" +
                            ")",
                    null);
            System.out.println("创建表结果: " + result);
            assertNotNull("创建表结果不应为空", result);

            // 2. 插入数据
            System.out.println("\n2. 插入数据");
            result = client.executeSql(
                    "INSERT INTO test_table (name, age) VALUES (?, ?)",
                    new Object[] { "张三", 25 });
            System.out.println("插入第一条数据结果: " + result);
            assertTrue("插入应该影响1行", result instanceof Integer && (Integer) result == 1);

            result = client.executeSql(
                    "INSERT INTO test_table (name, age) VALUES (?, ?)",
                    new Object[] { "李四", 30 });
            System.out.println("插入第二条数据结果: " + result);

            // 3. 基本查询
            System.out.println("\n3. 基本查询");
            result = client.executeSql(
                    "SELECT * FROM test_table WHERE age > ?",
                    new Object[] { 20 });
            System.out.println("查询年龄大于20的结果: " + result);

            // 4. 聚合查询
            System.out.println("\n4. 聚合查询");
            result = client.executeSql(
                    "SELECT COUNT(*) as total, AVG(age) as avg_age FROM test_table",
                    null);
            System.out.println("聚合查询结果: " + result);

            // 5. 更新数据
            System.out.println("\n5. 更新数据");
            result = client.executeSql(
                    "UPDATE test_table SET age = age + 1 WHERE name = ?",
                    new Object[] { "张三" });
            System.out.println("更新结果: " + result);

            // 6. 验证更新
            System.out.println("\n6. 验证更新");
            result = client.executeSql(
                    "SELECT * FROM test_table WHERE name = ?",
                    new Object[] { "张三" });
            System.out.println("更新后查询结果: " + result);

            // 7. 删除数据
            System.out.println("\n7. 删除数据");
            result = client.executeSql(
                    "DELETE FROM test_table WHERE age > ?",
                    new Object[] { 28 });
            System.out.println("删除结果: " + result);

            // 8. 最终验证
            System.out.println("\n8. 最终验证");
            result = client.executeSql(
                    "SELECT * FROM test_table ORDER BY id",
                    null);
            System.out.println("最终数据: " + result);

        } catch (Exception e) {
            System.err.println("测试过程中出现错误: " + e.getMessage());
            throw e;
        } finally {
            try {
                // 清理测试数据
                System.out.println("\n9. 清理测试数据");
                client.executeSql("DROP TABLE IF EXISTS test_table", null);
            } catch (Exception e) {
                System.err.println("清理数据时出现错误: " + e.getMessage());
            }
        }
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