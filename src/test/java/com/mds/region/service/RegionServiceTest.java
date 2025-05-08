package com.mds.region.service;

import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import com.mds.region.service.impl.RegionServiceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RegionServiceTest {
    private RegionServiceImpl regionService;
    private static final String TEST_REGION_ID = "test-region-1";
    private static final String TEST_HOST = "localhost";
    private static final int TEST_PORT = 8080;
    private static final String TEST_TABLE_NAME = "test_table";

    @Before
    public void setUp() throws Exception {
        regionService = new RegionServiceImpl(TEST_REGION_ID);
        // 注册区域节点
        RegionInfo regionInfo = new RegionInfo();
        regionInfo.setRegionId(TEST_REGION_ID);
        regionInfo.setHost("localhost");
        regionInfo.setPort(8080);
        regionInfo.setStatus("ACTIVE");
        regionInfo.setCreateTime(System.currentTimeMillis());
        assertTrue(regionService.register(regionInfo));
    }

    @After
    public void tearDown() throws Exception {
        // 清理测试数据
        regionService.dropTable("test_table");
    }

    @Test
    public void testRegister() {
        RegionInfo regionInfo = new RegionInfo(TEST_REGION_ID, TEST_HOST, TEST_PORT);
        assertTrue(regionService.register(regionInfo));
        
        RegionInfo retrievedInfo = regionService.getRegionInfo(TEST_REGION_ID);
        assertNotNull(retrievedInfo);
        assertEquals(TEST_REGION_ID, retrievedInfo.getRegionId());
        assertEquals(TEST_HOST, retrievedInfo.getHost());
        assertEquals(TEST_PORT, retrievedInfo.getPort());
    }

    @Test
    public void testHeartbeat() {
        RegionInfo regionInfo = new RegionInfo(TEST_REGION_ID, TEST_HOST, TEST_PORT);
        regionService.register(regionInfo);
        
        assertTrue(regionService.heartbeat(TEST_REGION_ID));
        
        RegionInfo retrievedInfo = regionService.getRegionInfo(TEST_REGION_ID);
        assertNotNull(retrievedInfo);
        assertTrue(retrievedInfo.getLastHeartbeat() > 0);
    }

    @Test
    public void testGetAllRegions() {
        RegionInfo regionInfo = new RegionInfo(TEST_REGION_ID, TEST_HOST, TEST_PORT);
        regionService.register(regionInfo);
        
        List<RegionInfo> regions = regionService.getAllRegions();
        assertNotNull(regions);
        assertFalse(regions.isEmpty());
        assertEquals(TEST_REGION_ID, regions.get(0).getRegionId());
    }

    @Test
    public void testCreateAndDropTable() {
        // 创建表
        TableInfo tableInfo = new TableInfo(
            "test_table",
            TEST_REGION_ID,
            Arrays.asList("id", "name", "age"),
            "id"
        );
        
        assertTrue(regionService.createTable(tableInfo));
        
        // 验证表信息
        TableInfo retrievedTable = regionService.getTableInfo("test_table");
        assertNotNull(retrievedTable);
        assertEquals("test_table", retrievedTable.getTableName());
        assertEquals(TEST_REGION_ID, retrievedTable.getRegionId());
        
        // 删除表
        assertTrue(regionService.dropTable("test_table"));
        
        // 验证表已被删除
        assertNull(regionService.getTableInfo("test_table"));
    }

    @Test
    public void testGetTablesByRegion() {
        // 创建测试表
        TableInfo tableInfo = new TableInfo(
            "test_table",
            TEST_REGION_ID,
            Arrays.asList("id", "name", "age"),
            "id"
        );
        regionService.createTable(tableInfo);
        
        // 获取区域节点上的表
        List<TableInfo> tables = regionService.getTablesByRegion(TEST_REGION_ID);
        assertNotNull(tables);
        assertFalse(tables.isEmpty());
        assertEquals("test_table", tables.get(0).getTableName());
    }

    @Test
    public void testTableOperations() throws Exception {
        // 创建表
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(TEST_TABLE_NAME);
        tableInfo.setRegionId(TEST_REGION_ID);
        tableInfo.setColumns(Arrays.asList("id", "name", "age"));
        tableInfo.setPrimaryKey("id");
        tableInfo.setCreateTime(System.currentTimeMillis());
        tableInfo.setStatus("ACTIVE");
        assertTrue(regionService.createTable(tableInfo));

        // 验证表信息
        TableInfo retrievedTable = regionService.getTableInfo(TEST_TABLE_NAME);
        assertNotNull(retrievedTable);
        assertEquals(TEST_TABLE_NAME, retrievedTable.getTableName());
        assertEquals(TEST_REGION_ID, retrievedTable.getRegionId());

        // 删除表
        assertTrue(regionService.dropTable(TEST_TABLE_NAME));
        assertNull(regionService.getTableInfo(TEST_TABLE_NAME));
    }

    @Test
    public void testDataOperations() throws Exception {
        // 创建测试表
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(TEST_TABLE_NAME);
        tableInfo.setRegionId(TEST_REGION_ID);
        tableInfo.setColumns(Arrays.asList("id", "name", "age"));
        tableInfo.setPrimaryKey("id");
        tableInfo.setCreateTime(System.currentTimeMillis());
        tableInfo.setStatus("ACTIVE");
        assertTrue(regionService.createTable(tableInfo));

        try {
            // 测试插入数据
            Map<String, Object> data = new HashMap<>();
            data.put("id", "1");
            data.put("name", "张三");
            data.put("age", "25");
            assertTrue(regionService.insert(TEST_TABLE_NAME, data));

            // 测试批量插入
            List<Map<String, Object>> batchData = new ArrayList<>();
            for (int i = 2; i <= 5; i++) {
                Map<String, Object> row = new HashMap<>();
                row.put("id", String.valueOf(i));
                row.put("name", "用户" + i);
                row.put("age", String.valueOf(20 + i));
                batchData.add(row);
            }
            assertTrue(regionService.batchInsert(TEST_TABLE_NAME, batchData));

            // 测试查询数据
            List<Map<String, Object>> results = regionService.query(TEST_TABLE_NAME, 
                    Arrays.asList("id", "name", "age"), null);
            assertEquals(5, results.size());

            // 测试条件查询
            results = regionService.query(TEST_TABLE_NAME, 
                    Arrays.asList("id", "name"), "age > '22'");
            assertEquals(4, results.size());

            // 测试分页查询
            results = regionService.queryWithPagination(TEST_TABLE_NAME, 
                    Arrays.asList("id", "name", "age"), null, 1, 2);
            assertEquals(2, results.size());

            // 测试更新数据
            Map<String, Object> updateData = new HashMap<>();
            updateData.put("age", "30");
            assertTrue(regionService.update(TEST_TABLE_NAME, updateData, "id = '1'"));

            // 验证更新结果
            results = regionService.query(TEST_TABLE_NAME, 
                    Arrays.asList("age"), "id = '1'");
            assertEquals("30", results.get(0).get("age"));

            // 测试删除数据
            assertTrue(regionService.delete(TEST_TABLE_NAME, "id = '1'"));
            results = regionService.query(TEST_TABLE_NAME, null, null);
            assertEquals(4, results.size());

            // 测试记录数统计
            long count = regionService.count(TEST_TABLE_NAME, "age > '22'");
            assertEquals(3, count);

        } finally {
            // 清理测试表
            regionService.dropTable(TEST_TABLE_NAME);
        }
    }

    @Test
    public void testCustomSQL() throws Exception {
        // 创建测试表
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(TEST_TABLE_NAME);
        tableInfo.setRegionId(TEST_REGION_ID);
        tableInfo.setColumns(Arrays.asList("id", "name", "age"));
        tableInfo.setPrimaryKey("id");
        tableInfo.setCreateTime(System.currentTimeMillis());
        tableInfo.setStatus("ACTIVE");
        assertTrue(regionService.createTable(tableInfo));

        try {
            // 测试执行自定义SQL
            String insertSQL = "INSERT INTO " + TEST_TABLE_NAME + " (id, name, age) VALUES ('1', '张三', '25')";
            assertTrue(regionService.execute(insertSQL));

            // 测试执行查询SQL
            String querySQL = "SELECT * FROM " + TEST_TABLE_NAME + " WHERE id = '1'";
            List<Map<String, Object>> results = regionService.executeQuery(querySQL);
            assertEquals(1, results.size());
            assertEquals("张三", results.get(0).get("column2")); // name列
            assertEquals("25", results.get(0).get("column3")); // age列

        } finally {
            // 清理测试表
            regionService.dropTable(TEST_TABLE_NAME);
        }
    }

    @Test
    public void testUpdateRouteInfo() {
        // 创建测试表
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(TEST_TABLE_NAME);
        tableInfo.setRegionId(TEST_REGION_ID);
        tableInfo.setColumns(Arrays.asList("id", "name", "age"));
        tableInfo.setPrimaryKey("id");
        tableInfo.setCreateTime(System.currentTimeMillis());
        tableInfo.setStatus("ACTIVE");
        assertTrue(regionService.createTable(tableInfo));

        try {
            // 测试更新路由信息
            assertTrue(regionService.updateRouteInfo());
            
            // 验证表路由信息已更新
            TableInfo updatedTable = regionService.getTableInfo(TEST_TABLE_NAME);
            assertNotNull(updatedTable);
            assertEquals(TEST_REGION_ID, updatedTable.getRegionId());
        } finally {
            regionService.dropTable(TEST_TABLE_NAME);
        }
    }

    @Test
    public void testReportTableDistribution() {
        // 创建测试表
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(TEST_TABLE_NAME);
        tableInfo.setRegionId(TEST_REGION_ID);
        tableInfo.setColumns(Arrays.asList("id", "name", "age"));
        tableInfo.setPrimaryKey("id");
        tableInfo.setCreateTime(System.currentTimeMillis());
        tableInfo.setStatus("ACTIVE");
        assertTrue(regionService.createTable(tableInfo));

        try {
            // 测试报告表分布
            assertTrue(regionService.reportTableDistribution());
            
            // 验证表分布信息已报告
            List<TableInfo> tables = regionService.getTablesByRegion(TEST_REGION_ID);
            assertFalse(tables.isEmpty());
            assertEquals(TEST_TABLE_NAME, tables.get(0).getTableName());
        } finally {
            regionService.dropTable(TEST_TABLE_NAME);
        }
    }

    @Test
    public void testDataMigration() {
        // 创建源表
        TableInfo sourceTable = new TableInfo();
        sourceTable.setTableName(TEST_TABLE_NAME);
        sourceTable.setRegionId(TEST_REGION_ID);
        sourceTable.setColumns(Arrays.asList("id", "name", "age"));
        sourceTable.setPrimaryKey("id");
        sourceTable.setCreateTime(System.currentTimeMillis());
        sourceTable.setStatus("ACTIVE");
        assertTrue(regionService.createTable(sourceTable));

        try {
            // 插入测试数据
            Map<String, Object> data = new HashMap<>();
            data.put("id", "1");
            data.put("name", "张三");
            data.put("age", "25");
            assertTrue(regionService.insert(TEST_TABLE_NAME, data));

            // 测试数据迁移请求
            String targetRegionId = "test-region-2";
            assertTrue(regionService.requestDataMigration(TEST_REGION_ID, targetRegionId, TEST_TABLE_NAME));

            // 测试执行数据迁移
            assertTrue(regionService.executeDataMigration(TEST_REGION_ID, targetRegionId, TEST_TABLE_NAME));

            // 验证源表数据
            List<Map<String, Object>> sourceData = regionService.query(TEST_TABLE_NAME, null, null);
            assertFalse(sourceData.isEmpty());
            assertEquals("1", sourceData.get(0).get("id"));
            assertEquals("张三", sourceData.get(0).get("name"));
            assertEquals("25", sourceData.get(0).get("age"));
        } finally {
            regionService.dropTable(TEST_TABLE_NAME);
        }
    }

    @Test
    public void testHandleMasterCommand() {
        // 测试处理Master命令
        String command = "REPORT_STATUS";
        assertTrue(regionService.handleMasterCommand(command));

        // 测试无效命令
        String invalidCommand = "INVALID_COMMAND";
        assertFalse(regionService.handleMasterCommand(invalidCommand));
    }
} 