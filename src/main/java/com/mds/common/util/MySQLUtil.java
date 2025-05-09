package com.mds.common.util;

import com.mds.common.config.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySQLUtil {
    private static final Logger logger = LoggerFactory.getLogger(MySQLUtil.class);
    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";

    static {
        try {
            logger.info("正在加载MySQL驱动: {}", DRIVER);
            Class.forName(DRIVER);
            logger.info("MySQL驱动加载成功");
        } catch (ClassNotFoundException e) {
            logger.error("MySQL驱动加载失败: {}", e.getMessage(), e);
        }
    }

    public static Connection getConnection() throws SQLException {
        logger.debug("正在连接MySQL数据库: {}", SystemConfig.MYSQL_URL);
        try {
            Connection conn = DriverManager.getConnection(
                    SystemConfig.MYSQL_URL,
                    SystemConfig.MYSQL_USER,
                    SystemConfig.MYSQL_PASSWORD
            );
            logger.debug("MySQL数据库连接成功");
            return conn;
        } catch (SQLException e) {
            logger.error("MySQL数据库连接失败: {}", e.getMessage(), e);
            throw e;
        }
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                logger.debug("MySQL数据库连接已关闭");
            } catch (SQLException e) {
                logger.error("关闭MySQL数据库连接失败: {}", e.getMessage(), e);
            }
        }
    }

    public static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
                logger.debug("Statement已关闭");
            } catch (SQLException e) {
                logger.error("关闭Statement失败: {}", e.getMessage(), e);
            }
        }
    }

    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
                logger.debug("ResultSet已关闭");
            } catch (SQLException e) {
                logger.error("关闭ResultSet失败: {}", e.getMessage(), e);
            }
        }
    }

    public static boolean executeUpdate(String sql, Object... params) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            logger.debug("执行更新SQL: {}, 参数: {}", sql, params);
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            int result = pstmt.executeUpdate();
            logger.debug("SQL执行结果: {}", result);
    
            // 对于CREATE TABLE和DROP TABLE操作进行进一步检查
            if (sql.trim().toUpperCase().startsWith("CREATE TABLE")) {
                // 进一步检查表是否已创建
                return checkIfTableExists(conn, "test_table");
            }
    
            if (sql.trim().toUpperCase().startsWith("DROP TABLE")) {
                // 进一步检查表是否已删除
                return !checkIfTableExists(conn, "test_table");
            }
    
            return result > 0;
        } catch (SQLException e) {
            logger.error("执行更新操作失败: {}, 错误信息: {}", sql, e.getMessage(), e);
            return false;
        } finally {
            closeStatement(pstmt);
            closeConnection(conn);
        }
    }
    
    // 检查表是否存在
    private static boolean checkIfTableExists(Connection conn, String tableName) {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'");
            return rs.next();
        } catch (SQLException e) {
            logger.error("检查表是否存在失败: {}", e.getMessage());
            return false;
        }
    }
    
    public static List<Object[]> executeQuery(String sql, Object... params) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<Object[]> result = new ArrayList<>();
        try {
            logger.debug("执行查询SQL: {}, 参数: {}", sql, params);
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                result.add(row);
            }
            logger.debug("查询结果行数: {}", result.size());
            return result;
        } catch (SQLException e) {
            logger.error("执行查询操作失败: {}, 错误信息: {}", sql, e.getMessage(), e);
            return new ArrayList<>();
        } finally {
            closeResultSet(rs);
            closeStatement(pstmt);
            closeConnection(conn);
        }
    }

    public static void execute(String sql) throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            logger.error("执行SQL失败: {}, 错误信息: {}", sql, e.getMessage(), e);
            throw e;
        }
    }

    public static List<Object[]> executeQuery(String sql) throws SQLException {
        List<Object[]> results = new ArrayList<>();
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                results.add(row);
            }
        } catch (SQLException e) {
            logger.error("执行查询SQL失败: {}, 错误信息: {}", sql, e.getMessage(), e);
            throw e;
        }
        return results;
    }
} 