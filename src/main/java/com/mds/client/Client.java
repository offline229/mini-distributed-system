package com.mds.client;

import com.mds.client.handler.MasterHandler;
import com.mds.client.handler.RegionHandler;
import com.mds.client.handler.ZookeeperHandler;
import org.json.JSONObject;

public class Client {
    private ZookeeperHandler zkHandler;
    private MasterHandler masterHandler;
    private RegionHandler regionHandler;

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

        // 3. 初始化Region处理器
        regionHandler = new RegionHandler();

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
        // 1. 向Master请求获取Region信息
        JSONObject request = new JSONObject();
        request.put("type", "GET_REGION");
        request.put("sql", sql);

        JSONObject response = masterHandler.sendRequest(request);
        String regionHost = response.getString("host");
        int regionPort = response.getInt("port");

        try {
            // 2. 连接Region并执行SQL
            regionHandler.connect(regionHost, regionPort);

            // 3. 发送SQL请求到Region
            JSONObject sqlRequest = new JSONObject();
            sqlRequest.put("operation", getSqlOperation(sql));
            sqlRequest.put("sql", sql);
            if (params != null) {
                sqlRequest.put("params", params);
            }

            // 4. 获取执行结果
            return regionHandler.sendRequest(sqlRequest);
        } catch (Exception e) {
            System.out.println("执行SQL失败: {}" + e.getMessage());
            throw e;
        }

    }

    private String getSqlOperation(String sql) {
        sql = sql.trim().toUpperCase();
        if (sql.startsWith("SELECT"))
            return "QUERY";
        if (sql.startsWith("INSERT"))
            return "INSERT";
        if (sql.startsWith("UPDATE"))
            return "UPDATE";
        if (sql.startsWith("DELETE"))
            return "DELETE";
        throw new IllegalArgumentException("不支持的SQL操作");
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
        // 直接连接Region
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
}