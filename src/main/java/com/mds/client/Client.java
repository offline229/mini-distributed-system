package com.mds.client;

import com.mds.client.handler.MasterHandler;
import com.mds.client.handler.RegionServerHandler;
import com.mds.client.handler.ZookeeperHandler;

import java.util.Scanner;

import org.json.JSONObject;

public class Client {
    private ZookeeperHandler zkHandler;
    private MasterHandler masterHandler;
    private RegionServerHandler regionHandler;

    private static final String DEFAULT_MASTER_HOST = "localhost";
    private static final int DEFAULT_MASTER_PORT = 9000;

    public void start() throws Exception {
        // 1. 初始化ZK处理器
        zkHandler = new ZookeeperHandler();
        zkHandler.init();

        // 2. 初始化Master处理器
        masterHandler = new MasterHandler();
        masterHandler.setTestMode(true); // 设置为测试模式
        initMasterConnection();

        // 3. 初始化RegionServer处理器
        regionHandler = new RegionServerHandler();

        System.out.println("Client启动成功");
    }

    private void initMasterConnection() {
        try {
            // 从ZK获取Master信息
            String masterInfo = zkHandler.getMasterInfo();
            System.out.println("从ZK获取的Master信息: " + masterInfo);
            if (masterInfo != null) {
                JSONObject json = new JSONObject(masterInfo);
                masterHandler.connect(json.getString("host"), json.getInt("port"));
            } else {
                // 使用默认配置
                System.out.println("使用默认Master配置");
                masterHandler.connect(DEFAULT_MASTER_HOST, DEFAULT_MASTER_PORT);
            }
        } catch (Exception e) {
            System.out.println("连接Master失败，使用默认配置: " + e.getMessage());
            try {
                masterHandler.connect(DEFAULT_MASTER_HOST, DEFAULT_MASTER_PORT);
            } catch (Exception ex) {
                System.err.println("连接默认Master也失败: " + ex.getMessage());
            }
        }
    }

    public Object executeSql(String sql, Object[] params) throws Exception {
        // 1. 向Master请求获取RegionServer信息
        JSONObject request = new JSONObject();
        request.put("type", "SQL");
        request.put("sql", sql);
        if (params != null) {
            request.put("params", params);
        }

        System.out.println("发送请求到Master: " + request.toString(2));

        // 2. 获取Master响应
        JSONObject response = masterHandler.sendRequest(request);
        if (response == null) {
            throw new Exception("从Master获取响应为空");
        }
        System.out.println("收到Master响应: " + response.toString(2));

        // 3. 检查响应状态
        if ("error".equals(response.getString("status"))) {
            throw new Exception(response.getString("message"));
        }

        // 4. 获取RegionServer信息并执行
        String regionHost = response.getString("host");
        int regionPort = response.getInt("port");
        System.out.println("获取到RegionServer地址: " + regionHost + ":" + regionPort);

        // 5. 直接执行SQL
        return executeOnRegionServer(sql, params, regionHost, regionPort);
    }

    public Object executeOnRegionServer(String sql, Object[] params, String host, int port) throws Exception {
        try {
            // 连接RegionServer
            regionHandler.connect(host, port);
            System.out.println("成功连接到RegionServer");

            // 构建SQL请求
            JSONObject sqlRequest = new JSONObject();
            sqlRequest.put("type", "CLIENT_REQUEST"); // 添加请求类型
            sqlRequest.put("operation", getSqlOperation(sql));
            sqlRequest.put("sql", sql);
            if (params != null) {
                sqlRequest.put("params", params);
            }

            // 发送请求并获取结果
            return regionHandler.sendRequest(sqlRequest);
        } catch (Exception e) {
            System.err.println("RegionServer执行失败: " + e.getMessage());
            throw e;
        }
    }

    private String getSqlOperation(String sql) {
        sql = sql.trim().toUpperCase();
        // DDL操作
        if (sql.startsWith("CREATE") ||
                sql.startsWith("DROP") ||
                sql.startsWith("ALTER") ||
                sql.startsWith("TRUNCATE")) {
            return "DDL";
        }
        // DML和DQL操作
        if (sql.startsWith("SELECT"))
            return "QUERY";
        if (sql.startsWith("INSERT"))
            return "INSERT";
        if (sql.startsWith("UPDATE"))
            return "UPDATE";
        if (sql.startsWith("DELETE"))
            return "DELETE";

        throw new IllegalArgumentException("不支持的SQL操作: " + sql);
    }

    public void stop() {
        if (regionHandler != null)
            regionHandler.close();
        if (masterHandler != null)
            masterHandler.close();
        if (zkHandler != null)
            zkHandler.close();
        System.out.println("Client已关闭");
    }

    // 添加直接执行方法，跳过Master
    public Object executeDirectly(String sql, Object[] params, String host, int port) throws Exception {
        // 直接连接RegionServer
        regionHandler.connect(host, port);

        // 构建SQL请求
        JSONObject sqlRequest = new JSONObject();
        sqlRequest.put("operation", getSqlOperation(sql));
        sqlRequest.put("sql", sql);
        if (params != null) {
            sqlRequest.put("params", params);
        }

        // 发送请求并获取结果
        return regionHandler.sendRequest(sqlRequest);
    }

    /*
     * 允许的SQL语句示例：
     * DELETE FROM users WHERE id = 1
     * UPDATE users SET name = "更新用户", age = 30 WHERE id = 1
     * CREATE TABLE users1 (id INT PRIMARY KEY, nameVARCHAR(50), age INT)
     * INSERT INTO users (id, name, age) VALUES (1, "测试用户1", 25)
     * SELECT * FROM users WHERE id = 1
     * SELECT * FROM users WHERE age > 20
     * 
     * SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users) AND id IN
     * (SELECT id FROM users WHERE name LIKE '%Test%')
     * 
     * SELECT age, COUNT(*) as user_count, MIN(id) as min_id, MAX(id) as max_id FROM
     * users GROUP BY age HAVING user_count > 0 ORDER BY age
     * 
     */

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        Client client = new Client();
        boolean directMode = false;

        try {
            // 1. 获取连接模式
            System.out.println("=== 分布式数据库客户端 ===");
            System.out.print("是否禁用Master直接与RegionServer通信? (1:是, 0:否): ");
            String modeStr = scanner.nextLine().trim();
            directMode = "1".equals(modeStr);

            if (directMode) {
                // 直接通信模式
                System.out.println("\n=== 直接通信模式 ===");
                client.regionHandler = new RegionServerHandler();
                System.out.println("将直接连接 RegionServer: localhost:8001");
            } else {
                // 通过Master通信模式
                System.out.println("\n=== Master通信模式 ===");
                client.start(); // 这里会自动从ZK获取Master信息
            }

            // 2. 进入命令行循环
            System.out.println("\n=== 输入SQL命令（输入'Quit'退出） ===");
            while (true) {
                System.out.print("\nsql> ");
                String input = scanner.nextLine().trim();

                if ("Quit".equalsIgnoreCase(input)) {
                    break;
                }

                try {
                    if (!input.isEmpty()) {
                        Object result;
                        if (directMode) {
                            result = client.executeDirectly(input, null, "localhost", 8001);
                        } else {
                            result = client.executeSql(input, null);
                        }

                        System.out.println("执行结果:");
                        if (result != null) {
                            if (result instanceof String && ((String) result).startsWith("{")) {
                                JSONObject json = new JSONObject((String) result);
                                System.out.println(json.toString(2));
                            } else {
                                System.out.println(result);
                            }
                        } else {
                            System.out.println("(无返回结果)");
                        }
                    }
                } catch (Exception e) {
                    System.err.println("SQL执行失败: " + e.getMessage());
                }
            }

        } catch (Exception e) {
            System.err.println("客户端运行错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.stop();
            scanner.close();
        }
    }
}