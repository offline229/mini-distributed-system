package com.mds.region;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class RegionServerShardingTest {
    private RegionServer server;

    @Before
    public void setUp() {
        server = new RegionServer("localhost", 8000, "1");
        server.start();
        System.out.println("\n=== 分片信息 ===");
        server.printShardingInfo();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testQueryWithShardKey() throws Exception {
        // 测试带分片键的查询
        String sql = "SELECT * FROM users WHERE id = 5";
        System.out.println("\n测试1: 带分片键的查询");
        System.out.println("执行SQL: " + sql);

        Object result = server.handleRequest(sql, null);

        assertNotNull("查询结果不应为空", result);
        System.out.println("查询结果: " + result);
    }

    @Test
    public void testQueryWithoutShardKey() throws Exception {
        // 测试不带分片键的查询
        String sql = "SELECT * FROM users";
        System.out.println("\n测试2: 不带分片键的查询");
        System.out.println("执行SQL: " + sql);

        Object result = server.handleRequest(sql, null);

        assertNotNull("查询结果不应为空", result);
        System.out.println("查询结果: " + result);
    }

    @Test
    public void testInsertOperation() throws Exception {
        // 测试插入操作
        String sql = "INSERT INTO users(id, name, age) VALUES(?, ?, ?)";
        Object[] params = { 11, "tet_user", 25 };
        System.out.println("\n测试3: 插入操作");
        System.out.println("执行SQL: " + sql + ", 参数: [15, 'test_user', 25]");

        Object result = server.handleRequest(sql, params);

        assertNotNull("插入结果不应为空", result);
        System.out.println("插入结果: " + result);
    }

    @Test
    public void testUpdateOperation() throws Exception {
        // 测试更新操作
        String sql = "UPDATE users SET name = ? WHERE id = ?";
        Object[] params = { "updated_user", 15 };
        System.out.println("\n测试4: 更新操作");
        System.out.println("执行SQL: " + sql + ", 参数: ['updated_user', 15]");

        Object result = server.handleRequest(sql, params);

        assertNotNull("更新结果不应为空", result);
        System.out.println("更新结果: " + result);
    }
}