package com.mds.region;

import com.mds.common.model.RegionInfo;
import com.mds.common.util.ZookeeperUtil;
import com.mds.common.config.SystemConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RegionRegistrationTest {
    private static final String TEST_REGION_ID = "test-region-1";
    private static final String TEST_HOST = "localhost";
    private static final int TEST_PORT = 8080;
    private static final String ZK_CONNECT_STRING = "localhost:2181";
    private static final int ZK_TIMEOUT = 5000;

    private ZookeeperUtil zkUtil;
    private Region region;
    private CountDownLatch registrationLatch;

    @Before
    public void setUp() throws Exception {
        zkUtil = new ZookeeperUtil();
        zkUtil.connect(ZK_CONNECT_STRING, ZK_TIMEOUT);
        region = new Region(TEST_REGION_ID, TEST_HOST, TEST_PORT, ZK_CONNECT_STRING);
        registrationLatch = new CountDownLatch(1);
    }

    @After
    public void tearDown() throws Exception {
        if (region != null) {
            region.stop();
        }
        if (zkUtil != null) {
            zkUtil.close();
        }
    }

    @Test
    public void testRegionRegistration() throws Exception {
        // 启动Region
        region.start();
        assertTrue(registrationLatch.await(5, TimeUnit.SECONDS));

        // 验证ZooKeeper注册
        String regionPath = SystemConfig.ZK_REGION_PATH + "/" + TEST_REGION_ID;
        assertTrue(zkUtil.exists(regionPath));

        // 验证注册信息
        byte[] data = zkUtil.getData(regionPath);
        assertNotNull(data);
        String regionInfoStr = new String(data);
        assertTrue(regionInfoStr.contains(TEST_REGION_ID));
        assertTrue(regionInfoStr.contains(TEST_HOST));
        assertTrue(regionInfoStr.contains(String.valueOf(TEST_PORT)));
    }

    @Test
    public void testRegionUnregistration() throws Exception {
        // 启动Region
        region.start();
        assertTrue(registrationLatch.await(5, TimeUnit.SECONDS));

        // 验证注册
        String regionPath = SystemConfig.ZK_REGION_PATH + "/" + TEST_REGION_ID;
        assertTrue(zkUtil.exists(regionPath));

        // 停止Region
        region.stop();

        // 验证注销
        assertFalse(zkUtil.exists(regionPath));
    }

    @Test
    public void testMultipleRegionRegistration() throws Exception {
        // 创建多个Region实例
        Region[] regions = new Region[3];
        for (int i = 0; i < regions.length; i++) {
            String regionId = "test-region-" + i;
            regions[i] = new Region(regionId, TEST_HOST, TEST_PORT + i, ZK_CONNECT_STRING);
            regions[i].start();
        }

        // 验证所有Region都已注册
        for (int i = 0; i < regions.length; i++) {
            String regionPath = SystemConfig.ZK_REGION_PATH + "/test-region-" + i;
            assertTrue(zkUtil.exists(regionPath));
        }

        // 停止所有Region
        for (Region r : regions) {
            r.stop();
        }

        // 验证所有Region都已注销
        for (int i = 0; i < regions.length; i++) {
            String regionPath = SystemConfig.ZK_REGION_PATH + "/test-region-" + i;
            assertFalse(zkUtil.exists(regionPath));
        }
    }

    @Test
    public void testRegionReconnection() throws Exception {
        // 启动Region
        region.start();
        assertTrue(registrationLatch.await(5, TimeUnit.SECONDS));

        // 验证注册
        String regionPath = SystemConfig.ZK_REGION_PATH + "/" + TEST_REGION_ID;
        assertTrue(zkUtil.exists(regionPath));

        // 模拟ZooKeeper连接断开
        zkUtil.close();
        Thread.sleep(1000);

        // 重新连接ZooKeeper
        zkUtil = new ZookeeperUtil();
        zkUtil.connect(ZK_CONNECT_STRING, ZK_TIMEOUT);

        // 验证Region仍然存在
        assertTrue(zkUtil.exists(regionPath));
    }

    @Test
    public void testRegionInfoUpdate() throws Exception {
        // 启动Region
        region.start();
        assertTrue(registrationLatch.await(5, TimeUnit.SECONDS));

        // 验证初始注册信息
        String regionPath = SystemConfig.ZK_REGION_PATH + "/" + TEST_REGION_ID;
        byte[] initialData = zkUtil.getData(regionPath);
        assertNotNull(initialData);

        // 等待一段时间
        Thread.sleep(1000);

        // 验证注册信息已更新
        byte[] updatedData = zkUtil.getData(regionPath);
        assertNotNull(updatedData);
        assertNotEquals(new String(initialData), new String(updatedData));
    }
} 