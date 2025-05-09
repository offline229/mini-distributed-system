package com.mds.region.handler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterHandler {
    private Socket masterSocket;
    private String masterHost = "localhost";
    private int masterPort = 9000;
    private volatile boolean isRunning;
    private ExecutorService threadPool;

    public void start() throws IOException {
        threadPool = Executors.newFixedThreadPool(2);
        isRunning = true;
        System.out.println("MasterHandler 已启动");
    }

    private void handleMasterRequest(Socket socket) {
        // 处理来自 Master 的请求
        System.out.println("处理来自 Master 的请求...");
        // TODO: 实现具体的指令处理逻辑
    }

    public String registerWithMaster(String host, int port) throws IOException {
        try {
            // 连接到Master
            masterSocket = new Socket(masterHost, masterPort);
            System.out.println("尝试连接到Master: " + masterHost + ":" + masterPort);

            // 发送注册请求
            Map<String, Object> registerRequest = new HashMap<>();
            registerRequest.put("type", "REGISTER");
            registerRequest.put("host", host);
            registerRequest.put("port", port);

            // 发送请求并等待响应
            sendRequest(registerRequest);

            // 测试阶段：直接返回测试ID
            System.out.println("Master未启动，使用测试ID：regiontest-1");
            return "regiontest-1";
        } catch (IOException e) {
            System.out.println("连接Master失败（预期行为，使用测试ID）");
            return "regiontest-1";
        }
    }

    private void sendRequest(Map<String, Object> request) throws IOException {
        try {
            if (masterSocket != null && masterSocket.isConnected()) {
                // TODO: 实际实现时添加序列化和发送逻辑
                System.out.println("发送请求到Master：" + request);
            } else {
                throw new IOException("未连接到Master");
            }
        } catch (Exception e) {
            if (masterSocket != null) {
                masterSocket.close();
            }
            throw new IOException("发送请求失败：" + e.getMessage());
        }
    }

    public void stop() {
        isRunning = false;
        if (masterSocket != null) {
            try {
                masterSocket.close();
            } catch (IOException e) {
                System.err.println("关闭Master连接失败：" + e.getMessage());
            }
        }
        if (threadPool != null) {
            threadPool.shutdown();
        }
        System.out.println("MasterHandler 已停止");
    }
}