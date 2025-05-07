package com.mds.region.service;

import com.mds.common.model.RegionInfo;
import com.mds.common.model.TableInfo;
import com.mds.region.service.impl.RegionServiceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

public class RegionServiceTest {
    private RegionServiceImpl regionService;
    private static final String TEST_REGION_ID = "test-region-1";
    private static final String TEST_HOST = "localhost";
    private static final int TEST_PORT = 8080;

    @Before
    public void setUp() throws Exception {
        regionService = new RegionServiceImpl(TEST_REGION_ID);
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
} 