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

    public Object execute(String sql, Object[] params) throws SQLException {
        sql = sql.trim().toUpperCase();
        try {
            // 解析SQL语句
            String[] words = sql.split("\\s+");
            if (words.length < 2) {
                throw new SQLException("无效的SQL语句");
            }

            String operation = words[0];
            String tableName = extractTableName(sql, operation);

            switch (operation) {
                case "SELECT":
                    return handleSelect(sql, tableName, params);
                case "INSERT":
                    return handleInsert(sql, tableName, params);
                case "UPDATE":
                    return handleUpdate(sql, tableName, params);
                case "DELETE":
                    return handleDelete(sql, tableName, params);
                default:
                    throw new SQLException("不支持的操作类型: " + operation);
            }
        } catch (Exception e) {
            logger.error("执行SQL失败: {}, 参数: {}", sql, params);
            throw new SQLException(e.getMessage(), e);
        }
    }

    private String extractTableName(String sql, String operation) {
        // 提取表名
        switch (operation) {
            case "SELECT":
                return sql.split("FROM\\s+")[1].split("\\s+")[0];
            case "INSERT":
                return sql.split("INTO\\s+")[1].split("\\s+")[0];
            case "UPDATE":
                return sql.split("UPDATE\\s+")[1].split("\\s+")[0];
            case "DELETE":
                return sql.split("FROM\\s+")[1].split("\\s+")[0];
            default:
                return "";
        }
    }

    private Object handleSelect(String sql, String tableName, Object[] params) {
        // 解析columns
        String[] columns = extractColumns(sql);
        // 解析where子句
        String whereClause = extractWhereClause(sql);
        // 复用query方法
        return query(tableName, columns, whereClause, params);
    }

    private Object handleInsert(String sql, String tableName, Object[] params) throws SQLException {
        try {
            // 解析columns和values
            String[] columns = extractInsertColumns(sql);
            if (columns.length != params.length) {
                throw new SQLException("列数与参数个数不匹配");
            }

            // 复用insert方法
            boolean success = insert(tableName, columns, params);
            return success ? 1 : 0;
        } catch (Exception e) {
            logger.error("处理INSERT失败: {}", e.getMessage());
            throw new SQLException("INSERT执行失败: " + e.getMessage(), e);
        }
    }

    private Object handleUpdate(String sql, String tableName, Object[] params) {
        // 解析columns, values和where子句
        String[] parts = sql.split("WHERE");
        String[] columns = extractUpdateColumns(parts[0]);
        String whereClause = parts.length > 1 ? parts[1].trim() : null;

        // 分割params为values和whereArgs
        Object[] values = new Object[columns.length];
        Object[] whereArgs = new Object[params.length - columns.length];
        System.arraycopy(params, 0, values, 0, columns.length);
        System.arraycopy(params, columns.length, whereArgs, 0, params.length - columns.length);

        // 复用update方法
        boolean success = update(tableName, columns, values, whereClause, whereArgs);
        return success ? 1 : 0;
    }

    private Object handleDelete(String sql, String tableName, Object[] params) {
        // 解析where子句
        String whereClause = extractWhereClause(sql);
        // 复用delete方法
        boolean success = delete(tableName, whereClause, params);
        return success ? 1 : 0;
    }

    private String[] extractColumns(String sql) {
        String columnsStr = sql.substring(sql.indexOf("SELECT") + 6, sql.indexOf("FROM")).trim();
        return columnsStr.equals("*") ? new String[0] : columnsStr.split(",\\s*");
    }

    private String[] extractInsertColumns(String sql) {
        try {
            // 提取 INSERT INTO table_name (col1, col2) 中的列名部分
            int startIndex = sql.indexOf("(") + 1;
            int endIndex = sql.indexOf(")");
            String columnsStr = sql.substring(startIndex, endIndex).trim();
            return columnsStr.split("\\s*,\\s*"); // 处理可能的空格
        } catch (Exception e) {
            logger.error("解析INSERT列名失败: {}", e.getMessage());
            throw new IllegalArgumentException("无法解析INSERT语句的列名", e);
        }
    }

    private String[] extractUpdateColumns(String sql) {
        String setClause = sql.substring(sql.indexOf("SET") + 3).trim();
        String[] parts = setClause.split(",\\s*");
        String[] columns = new String[parts.length];
        for (int i = 0; i < parts.length; i++) {
            columns[i] = parts[i].split("\\s*=\\s*")[0].trim();
        }
        return columns;
    }

    private String extractWhereClause(String sql) {
        String[] parts = sql.split("WHERE");
        return parts.length > 1 ? parts[1].trim() : null;
    }

    // 插入数据
    public boolean insert(String tableName, String[] columns, Object[] values) {
        try {
            // 1. 构建SQL语句
            StringBuilder sql = new StringBuilder()
                    .append("INSERT INTO ")
                    .append(tableName)
                    .append(" (");

            // 2. 添加列名
            for (int i = 0; i < columns.length; i++) {
                sql.append(columns[i].trim()); // 确保列名没有多余空格
                if (i < columns.length - 1) {
                    sql.append(", ");
                }
            }

            // 3. 添加VALUES子句
            sql.append(") VALUES (");
            for (int i = 0; i < values.length; i++) {
                sql.append("?");
                if (i < values.length - 1) {
                    sql.append(", ");
                }
            }
            sql.append(")");

            // 4. 执行SQL
            String finalSql = sql.toString();
            logger.debug("执行SQL: {}, 参数: {}", finalSql, values);
            return MySQLUtil.executeUpdate(finalSql, values);
        } catch (Exception e) {
            logger.error("插入数据失败: {}", e.getMessage());
            return false;
        }
    }

    // 查询数据
    public List<Object[]> query(String tableName, String[] columns, String whereClause, Object[] whereArgs) {
        StringBuilder sql = new StringBuilder("SELECT ");
        if (columns == null || columns.length == 0) {
            sql.append("*");
        } else {
            for (int i = 0; i < columns.length; i++) {
                sql.append(columns[i]);
                if (i < columns.length - 1)
                    sql.append(", ");
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
            if (i < columns.length - 1)
                sql.append(", ");
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