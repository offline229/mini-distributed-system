package com.mds.common.test;

import com.mds.common.util.ZookeeperUtil;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
// 检测common包中的ZookeeperUtil类是否能够正常工作
public class SimpleZkTest {
    private ZookeeperUtil zkUtil;
    private static final String TEST_PATH = "/test";
    private static final String TEST_DATA = "test data";

    @Before
    public void setUp() throws Exception {
        zkUtil = new ZookeeperUtil();
    }

    @After
    public void tearDown() throws Exception {
        if (zkUtil != null) {
            zkUtil.close();
        }
    }

    @Test
    public void testCreateAndGetData() throws Exception {
        // 创建节点
        zkUtil.createPath(TEST_PATH, CreateMode.PERSISTENT);
        assertTrue(zkUtil.exists(TEST_PATH));

        // 设置数据
        zkUtil.setData(TEST_PATH, TEST_DATA.getBytes());
        byte[] data = zkUtil.getData(TEST_PATH);
        assertNotNull(data);
        assertEquals(TEST_DATA, new String(data));

        // 删除节点
        zkUtil.delete(TEST_PATH);
        assertFalse(zkUtil.exists(TEST_PATH));
    }

    @Test
    public void testGetChildren() throws Exception {
        // 创建父节点
        zkUtil.createPath(TEST_PATH, CreateMode.PERSISTENT);
        
        // 创建子节点
        String childPath1 = TEST_PATH + "/child1";
        String childPath2 = TEST_PATH + "/child2";
        zkUtil.createPath(childPath1, CreateMode.PERSISTENT);
        zkUtil.createPath(childPath2, CreateMode.PERSISTENT);

        // 获取子节点列表
        assertTrue(zkUtil.getChildren(TEST_PATH).size() == 2);

        // 清理
        zkUtil.delete(childPath1);
        zkUtil.delete(childPath2);
        zkUtil.delete(TEST_PATH);
    }
} 