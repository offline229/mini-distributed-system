package com.mds.region;

import com.mds.client.Client;
import org.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class RegionServerIntegrationTest {
    private static final String HOST = "localhost";
    private static final int PORT = 8000;
    
    private RegionServer regionServer;
    private Client client;
    
    @Before
    public void setUp() {
        // 启动 RegionServer
        regionServer = new RegionServer(HOST, PORT);
        regionServer.start();
        
        // 初始化 Client
        client = new Client();
        try {
            client.start();
        } catch (Exception e) {
            fail("Client 启动失败: " + e.getMessage());
        }
    }
    
    @After
    public void tearDown() {
        if (client != null) {
            client.stop();
        }
        // RegionServer 的关闭逻辑会在后续实现
    }

    public void testCreateTable() throws Exception {
        String tableName = "test_users1";

        // 创建测试表
        String createTableSql = String.format("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(50), age INT)", tableName);
        Object result = client.executeDirectly(createTableSql, null, HOST, PORT);
        assertNotNull("创建表结果不应为空", result);

        // 验证表是否创建成功
        String checkTableSql = String.format("SELECT COUNT(*) FROM %s", tableName);
        Object countResult = client.executeDirectly(checkTableSql, null, HOST, PORT);
        assertNotNull("查询结果不应为空", countResult);

        // 直接修改assertEquals比较JSONArray的结果
        // countResult 应该是JSONArray，获取其第一个元素并提取COUNT(*)的值
        int count = ((JSONArray) countResult).getJSONObject(0).getInt("COUNT(*)");

        // 断言结果
        assertEquals("新创建的表应该没有数据", 0, count);
    }

    
    @Test
    public void testInsertAndQuery() throws Exception {
        String tableName = "test_insert";
        
        // 1. 先创建表
        String createTableSql = String.format("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(50), age INT)", tableName);
        Object createResult = client.executeDirectly(createTableSql, null, HOST, PORT);
        assertNotNull("创建表结果不应为空", createResult);
        
        // 2. 插入数据
        String insertSql = String.format("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", tableName);
        Object[] insertParams = {1, "测试用户1", 25};
        Object insertResult = client.executeDirectly(insertSql, insertParams, HOST, PORT);
        assertNotNull("插入结果不应为空", insertResult);
        
        // 3. 查询数据
        String querySql = String.format("SELECT * FROM %s WHERE id = ?", tableName);
        Object[] queryParams = {1};
        Object queryResult = client.executeDirectly(querySql, queryParams, HOST, PORT);
        assertNotNull("查询结果不应为空", queryResult);
    }

    @Test
    public void testUpdateAndDelete() throws Exception {
        String tableName = "test_crud";

        // 1. 创建表
        String createTableSql = String.format("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(50), age INT)", tableName);
        Object createResult = client.executeDirectly(createTableSql, null, HOST, PORT);
        assertNotNull("创建表结果不应为空", createResult);

        // 2. 插入初始数据
        String insertSql = String.format("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", tableName);
        Object[] insertParams = {1, "原始用户", 20};
        Object insertResult = client.executeDirectly(insertSql, insertParams, HOST, PORT);
        assertNotNull("插入结果不应为空", insertResult);

        // 3. 更新数据
        String updateSql = String.format("UPDATE %s SET name = ?, age = ? WHERE id = ?", tableName);
        Object[] updateParams = {"更新用户", 30, 1};
        Object updateResult = client.executeDirectly(updateSql, updateParams, HOST, PORT);
        assertNotNull("更新结果不应为空", updateResult);

        // 4. 验证更新
        String querySql = String.format("SELECT * FROM %s WHERE id = ?", tableName);
        Object[] queryParams = {1};
        Object queryResult = client.executeDirectly(querySql, queryParams, HOST, PORT);
        assertNotNull("查询结果不应为空", queryResult);

        // 5. 删除数据
        String deleteSql = String.format("DELETE FROM %s WHERE id = ?", tableName);
        Object[] deleteParams = {1};
        Object deleteResult = client.executeDirectly(deleteSql, deleteParams, HOST, PORT);
        assertNotNull("删除结果不应为空", deleteResult);

        // 6. 验证删除
        Object finalQueryResult = client.executeDirectly(querySql, queryParams, HOST, PORT);
        // 直接判断查询结果是否为空数组，使用assertTrue验证
        assertTrue("删除后查询结果应为空", ((JSONArray) finalQueryResult).isEmpty());
    }

    @Test
    public void testBatchOperations() throws Exception {
        String tableName = "test_batch";

        // 1. 创建表
        String createTableSql = String.format("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(50), age INT)", tableName);
        Object createResult = client.executeDirectly(createTableSql, null, HOST, PORT);
        assertNotNull("创建表结果不应为空", createResult);

        // 2. 批量插入数据
        for (int i = 1; i <= 5; i++) {
            String insertSql = String.format("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", tableName);
            Object[] insertParams = {i, "用户" + i, 20 + i};
            Object insertResult = client.executeDirectly(insertSql, insertParams, HOST, PORT);
            assertNotNull("插入结果不应为空", insertResult);
        }

        // 3. 批量查询
        String querySql = String.format("SELECT * FROM %s WHERE age > ?", tableName);
        Object[] queryParams = {22};
        Object queryResult = client.executeDirectly(querySql, queryParams, HOST, PORT);
        assertNotNull("批量查询结果不应为空", queryResult);
    }

    @Test
    public void testErrorHandling() {
        // 1. 测试无效的 SQL
        try {
            String invalidSql = "INVALID SQL STATEMENT";
            client.executeDirectly(invalidSql, null, HOST, PORT);
            fail("应该抛出异常");
        } catch (Exception e) {
            assertTrue("应该捕获到异常", e instanceof Exception);
        }

        // 2. 测试查询不存在的表
        try {
            String queryNonExistentTable = "SELECT * FROM non_existent_table";
            client.executeDirectly(queryNonExistentTable, null, HOST, PORT);
            fail("应该抛出异常");
        } catch (Exception e) {
            assertTrue("应该捕获到异常", e instanceof Exception);
        }
    }

}