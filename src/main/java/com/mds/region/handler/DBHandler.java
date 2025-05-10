package com.mds.region.handler;

import com.mds.common.util.MySQLUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DBHandler {
    private static final Logger logger = LoggerFactory.getLogger(DBHandler.class);
    private static final int QUERY_TIMEOUT = 30; // 查询超时时间（秒）
    private static final int MAX_ROWS = 1000; // 最大返回行数

    public void init() throws SQLException {
        try {
            MySQLUtil.getConnection().close();
            logger.info("数据库连接验证成功");
        } catch (SQLException e) {
            logger.error("数据库连接验证失败: {}", e.getMessage());
            throw e;
        }
    }

    public Object execute(String sql, Object[] params) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = MySQLUtil.getConnection();
            String upperSql = sql.trim().toUpperCase();
            String operation = upperSql.split("\\s+")[0];
            logger.debug("执行SQL操作: {}, 参数: {}", operation, params);

            // DDL操作
            if (isDDL(operation)) {
                stmt = conn.createStatement();
                stmt.execute(sql);
                return "SQL执行成功";
            }

            // DML和DQL操作
            pstmt = conn.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            if (operation.equals("SELECT")) {
                rs = pstmt.executeQuery();
                return resultSetToJson(rs);
            } else {
                return pstmt.executeUpdate();
            }

        } catch (SQLException e) {
            logger.error("SQL执行失败: {}, 参数: {}, 错误: {}", sql, params, e.getMessage());
            throw e;
        } finally {
            closeResources(rs, stmt, pstmt, conn);
        }
    }

    private boolean isDDL(String operation) {
        switch (operation) {
            case "CREATE":
            case "DROP":
            case "ALTER":
            case "TRUNCATE":
                return true;
            default:
                return false;
        }
    }

    private JSONArray resultSetToJson(ResultSet rs) throws SQLException {
        JSONArray jsonArray = new JSONArray();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            JSONObject row = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                Object value = rs.getObject(i);
                row.put(columnName, value != null ? value : JSONObject.NULL);
            }
            jsonArray.put(row);
        }
        return jsonArray;
    }

    private void closeResources(ResultSet rs, Statement stmt,
            PreparedStatement pstmt, Connection conn) {
        try {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
            if (pstmt != null)
                pstmt.close();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            logger.error("关闭资源失败: {}", e.getMessage());
        }
    }

    public void close() {
        logger.info("DBHandler关闭操作被调用");
    }
}