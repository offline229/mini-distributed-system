package com.mds.common.test;

import com.mds.common.config.SystemConfig;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConfigTest {
    @Test
    public void testZooKeeperConfig() {
        assertNotNull(SystemConfig.ZK_CONNECT_STRING);
        assertTrue(SystemConfig.ZK_SESSION_TIMEOUT > 0);
        assertNotNull(SystemConfig.ZK_ROOT_PATH);
        assertNotNull(SystemConfig.ZK_REGION_PATH);
        assertNotNull(SystemConfig.ZK_MASTER_PATH);
        assertNotNull(SystemConfig.ZK_TABLE_PATH);
    }

    @Test
    public void testMySQLConfig() {
        assertNotNull(SystemConfig.MYSQL_URL);
        assertNotNull(SystemConfig.MYSQL_USER);
        assertNotNull(SystemConfig.MYSQL_PASSWORD);
        assertTrue(SystemConfig.MYSQL_POOL_SIZE > 0);
    }

    @Test
    public void testSystemConfig() {
        assertTrue(SystemConfig.HEARTBEAT_INTERVAL > 0);
        assertTrue(SystemConfig.HEARTBEAT_TIMEOUT > 0);
        assertTrue(SystemConfig.REGION_PORT > 0);
        assertTrue(SystemConfig.MASTER_PORT > 0);
    }

    @Test
    public void testStatusConstants() {
        assertEquals("ONLINE", SystemConfig.STATUS_ONLINE);
        assertEquals("OFFLINE", SystemConfig.STATUS_OFFLINE);
        assertEquals("ACTIVE", SystemConfig.STATUS_ACTIVE);
        assertEquals("INACTIVE", SystemConfig.STATUS_INACTIVE);
    }
} 