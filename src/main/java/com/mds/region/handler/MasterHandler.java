package com.mds.region.handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;

public class MasterHandler {
    private Socket masterSocket;
    private String masterHost = "localhost";
    private int masterPort = 8000;
    private boolean testMode = false; // 添加测试模式标志
    private ExecutorService threadPool;
    private ZooKeeper zooKeeper; // 添加ZK客户端
    private static final int HEARTBEAT_INTERVAL = 5000; // 5秒一次心跳
    private volatile boolean isHeartbeatRunning;
    private Thread heartbeatThread;
    private String regionserverId; // 添加字段
    private String host; // 添加字段
    private int port; // 添加字段quiqui

    public void start() throws IOException {
        threadPool = Executors.newFixedThreadPool(2);
        startHeartbeat();
        System.out.println("心跳启动");
        System.out.println("MasterHandler 已启动" + (testMode ? "(测试模式)" : ""));
    }

    private void startHeartbeat() {
        if (testMode) {
            System.out.println("[测试模式] 跳过心跳启动");
            return;
        }

        isHeartbeatRunning = true;
        heartbeatThread = new Thread(() -> {
            while (isHeartbeatRunning) {
                try {
                    sendHeartbeat();
                    Thread.sleep(HEARTBEAT_INTERVAL);
                } catch (IOException e) {
                    System.err.println("发送心跳失败: " + e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "HeartbeatThread");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
        System.out.println("心跳线程已启动，间隔: " + HEARTBEAT_INTERVAL + "ms");
    }

    private void sendHeartbeat() throws IOException {
        if (testMode) {
            System.out.println("[测试模式] 跳过心跳发送");
            return;
        }

        // 构建心跳消息
        JSONObject heartbeat = new JSONObject();
        heartbeat.put("type", "HEARTBEAT");
        heartbeat.put("regionserverId", this.regionserverId); // 添加 regionserverId
        heartbeat.put("host", this.host); // 添加 host
        heartbeat.put("port", this.port); // 添加 port
        heartbeat.put("connections", 0); // 添加当前连接数
        heartbeat.put("timestamp", System.currentTimeMillis());
        heartbeat.put("status", "ACTIVE");

        // 直接发送心跳，不等待响应
        if (masterSocket != null && masterSocket.isConnected()) {
            PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
            out.println(heartbeat.toString());
            System.out.println("发送心跳到Master：" + heartbeat.toString(2));
        } else {
            throw new IOException("未连接到Master");
        }
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    public String registerRegionServer(String host, int port, String replicaKey) throws IOException {
        try {
            this.host = host;
            this.port = port;
            // 1. 从ZK获取活跃master节点列表
            List<String> activeNodes = zooKeeper.getChildren("/mds/master/active", false);
            if (activeNodes == null || activeNodes.isEmpty()) {
                System.out.println("ZK中未找到active master，使用默认测试配置");
                setTestMode(true);
                return "RegionServerTest-1";
            }

            // 2. 获取活跃master节点数据
            String activeMasterPath = "/mds/master/active/" + activeNodes.get(0);
            byte[] data = zooKeeper.getData(activeMasterPath, false, null);

            if (data == null) {
                System.out.println("Active master节点数据为空，使用测试配置");
                setTestMode(true);
                return "RegionServerTest-1";
            }

            // 3. 解析master信息
            JSONObject masterInfo = new JSONObject(new String(data));
            masterHost = masterInfo.getString("host");
            masterPort = masterInfo.getInt("port");
            System.out.println("从ZK获取到Master信息: " + masterHost + ":" + masterPort);

            // 4. 连接到Master
            try {
                masterSocket = new Socket(masterHost, masterPort);
                System.out.println("成功连接到Master: " + masterHost + ":" + masterPort);

                // 5. 构建注册请求
                JSONObject registerRequest = new JSONObject();
                registerRequest.put("type", "REGISTER");
                registerRequest.put("host", host);
                registerRequest.put("port", port);
                registerRequest.put("replicaKey", replicaKey);

                // 6. 发送注册请求
                // 获取响应并保存ID
                String id = sendRequestAndWaitResponse(registerRequest);
                this.regionserverId = id;

                // 注册成功后启动心跳
                startHeartbeat();

                return id;

            } catch (IOException e) {
                System.out.println("连接Master失败，进入测试模式: " + e.getMessage());
                setTestMode(true);
                return "RegionServerTest-1";
            }

        } catch (KeeperException | InterruptedException e) {
            System.out.println("ZK操作失败，进入测试模式: " + e.getMessage());
            setTestMode(true);
            return "RegionServerTest-1";
        }
    }

    private String sendRequestAndWaitResponse(JSONObject request) throws IOException {
        if (testMode) {
            System.out.println("[测试模式] 模拟发送请求到Master：" + request.toString(2));
            return "RegionServerTest-1";
        }

        try {
            if (masterSocket != null && masterSocket.isConnected()) {
                // 发送请求
                PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));

                out.println(request.toString());
                System.out.println("发送请求到Master：" + request.toString(2));

                // 等待响应
                String responseStr = in.readLine();
                System.out.println("收到Master响应：" + responseStr);

                // 解析响应
                JSONObject response = new JSONObject(responseStr);
                if ("ok".equals(response.getString("status"))) {
                    String regionserverId = response.getString("regionserverId");
                    System.out.println("注册成功，获得ID: " + regionserverId);
                    return regionserverId;
                } else {
                    throw new IOException("注册失败: " + response.getString("message"));
                }
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

    // 添加ZK初始化方法
    public void init() throws IOException {
        // ZK客户端初始化
        CountDownLatch connectedLatch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper("localhost:2181", 3000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedLatch.countDown();
            }
        });
        try {
            connectedLatch.await();
        } catch (InterruptedException e) {
            throw new IOException("ZK连接超时", e);
        }
    }

    private void sendRequest(Map<String, Object> request) throws IOException {
        // 测试模式下直接打印并返回，不实际发送
        if (testMode) {
            System.out.println("[测试模式] 模拟发送请求到Master：" + request);
            return;
        }

        // 正常模式下的发送逻辑
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
            throw new IOException("发送请求" + request + "失败：" + e.getMessage());
        }
    }

    public void stop() {
        isHeartbeatRunning = false;

        // 停止心跳线程
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
            try {
                heartbeatThread.join(1000); // 等待心跳线程结束
            } catch (InterruptedException e) {
                System.err.println("等待心跳线程结束被中断");
            }
        }
        if (masterSocket != null) {
            try {
                masterSocket.close();
            } catch (IOException e) {
                System.err.println("关闭Master连接失败：" + e.getMessage());
            }
        }
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                System.err.println("关闭ZK连接失败：" + e.getMessage());
            }
        }
        if (threadPool != null) {
            threadPool.shutdown();
        }
        System.out.println("MasterHandler 已停止");
    }
}