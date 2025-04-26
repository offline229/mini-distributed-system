package com.example.test;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLConnectTest {
    private static final Logger logger = LoggerFactory.getLogger(MySQLConnectTest.class);
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/test";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "64793416zhu";

    @Test
    public void testMySQLConnection() throws Exception {
        logger.info("开始测试 MySQL 连接...");
        
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {
            logger.info("MySQL 连接测试成功");
        }
    }
} 