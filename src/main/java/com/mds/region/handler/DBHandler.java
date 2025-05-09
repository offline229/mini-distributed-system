package com.mds.region.handler;

import com.mds.common.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class DBHandler {
    private static final Logger logger = LoggerFactory.getLogger(DBHandler.class);

    // 初始化时验证数据库连接
    public void init() throws SQLException {
        try {
            MySQLUtil.getConnection().close();
            logger.info("数据库连接验证成功");
        } catch (SQLException e) {
            logger.error("数据库连接验证失败: {}", e.getMessage());
            throw e;
        }
    }

    // 插入数据
    public boolean insert(String tableName, String[] columns, Object[] values) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        for (int i = 0; i < columns.length; i++) {
            sql.append(columns[i]);
            if (i < columns.length - 1) sql.append(", ");
        }
        sql.append(") VALUES (");
        for (int i = 0; i < values.length; i++) {
            sql.append("?");
            if (i < values.length - 1) sql.append(", ");
        }
        sql.append(")");

        return MySQLUtil.executeUpdate(sql.toString(), values);
    }

    // 查询数据
    public List<Object[]> query(String tableName, String[] columns, String whereClause, Object[] whereArgs) {
        StringBuilder sql = new StringBuilder("SELECT ");
        if (columns == null || columns.length == 0) {
            sql.append("*");
        } else {
            for (int i = 0; i < columns.length; i++) {
                sql.append(columns[i]);
                if (i < columns.length - 1) sql.append(", ");
            }
        }
        sql.append(" FROM ").append(tableName);
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }

        return MySQLUtil.executeQuery(sql.toString(), whereArgs != null ? whereArgs : new Object[0]);
    }

    // 更新数据
    public boolean update(String tableName, String[] columns, Object[] values, String whereClause, Object[] whereArgs) {
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        for (int i = 0; i < columns.length; i++) {
            sql.append(columns[i]).append(" = ?");
            if (i < columns.length - 1) sql.append(", ");
        }
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }

        Object[] params = new Object[values.length + (whereArgs != null ? whereArgs.length : 0)];
        System.arraycopy(values, 0, params, 0, values.length);
        if (whereArgs != null) {
            System.arraycopy(whereArgs, 0, params, values.length, whereArgs.length);
        }

        return MySQLUtil.executeUpdate(sql.toString(), params);
    }

    // 删除数据
    public boolean delete(String tableName, String whereClause, Object[] whereArgs) {
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName);
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }

        return MySQLUtil.executeUpdate(sql.toString(), whereArgs != null ? whereArgs : new Object[0]);
    }

    // 关闭操作（保留方法签名，但实际上不需要做任何事情，因为MySQLUtil已经处理了连接的关闭）
    public void close() {
        logger.info("DBHandler关闭操作被调用");
    }
}