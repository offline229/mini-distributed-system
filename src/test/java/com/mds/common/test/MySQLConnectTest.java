package com.mds.common.test;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
// 检测是否能够连接到本地的mysql的数据库
// 更改为自己的密码配置
public class MySQLConnectTest {
    private static final Logger logger = LoggerFactory.getLogger(MySQLConnectTest.class);
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/mini_distributed_system";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";

    @Test
    public void testMySQLConnection() throws Exception {
        logger.info("开始测试 MySQL 连接...");
        
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {
            logger.info("MySQL 连接测试成功");
        }
    }
} 