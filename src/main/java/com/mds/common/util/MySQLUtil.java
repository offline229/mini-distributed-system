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
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            logger.error("MySQL驱动加载失败", e);
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                SystemConfig.MYSQL_URL,
                SystemConfig.MYSQL_USER,
                SystemConfig.MYSQL_PASSWORD
        );
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("关闭数据库连接失败", e);
            }
        }
    }

    public static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error("关闭Statement失败", e);
            }
        }
    }

    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error("关闭ResultSet失败", e);
            }
        }
    }

    public static boolean executeUpdate(String sql, Object... params) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            return pstmt.executeUpdate() > 0;
        } catch (SQLException e) {
            logger.error("执行更新操作失败: {}", sql, e);
            return false;
        } finally {
            closeStatement(pstmt);
            closeConnection(conn);
        }
    }

    public static List<Object[]> executeQuery(String sql, Object... params) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<Object[]> result = new ArrayList<>();
        try {
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
            return result;
        } catch (SQLException e) {
            logger.error("执行查询操作失败: {}", sql, e);
            return new ArrayList<>();
        } finally {
            closeResultSet(rs);
            closeStatement(pstmt);
            closeConnection(conn);
        }
    }
} 