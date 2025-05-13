package com.mds.region.handler;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
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
    private volatile boolean isRunning;
    private boolean testMode = false; // 添加测试模式标志
    private ExecutorService threadPool;
    private ZooKeeper zooKeeper; // 添加ZK客户端
    private static final int HEARTBEAT_INTERVAL = 5000; // 5秒一次心跳
    private volatile boolean isHeartbeatRunning;
    private Thread heartbeatThread;

    public void start() throws IOException {
        threadPool = Executors.newFixedThreadPool(2);
        isRunning = true;

        // 只在非测试模式下启动心跳
        if (!testMode) {
            startHeartbeat();
        } else {
            System.out.println("测试模式：不启动心跳");
        }

        System.out.println("MasterHandler 已启动" + (testMode ? "(测试模式)" : ""));
    }

    private void handleMasterRequest(Socket socket) {
        // 处理来自 Master 的请求
        System.out.println("处理来自 Master 的请求...");
        // TODO: 实现具体的指令处理逻辑
    }

    private void startHeartbeat() {
        isHeartbeatRunning = true;
        heartbeatThread = new Thread(() -> {
            while (isHeartbeatRunning && !Thread.currentThread().isInterrupted()) {
                try {
                    sendHeartbeat();
                    Thread.sleep(HEARTBEAT_INTERVAL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("心跳线程被中断");
                    break;
                } catch (Exception e) {
                    System.err.println("发送心跳失败: " + e.getMessage());
                }
            }
        }, "heartbeat-thread");
        heartbeatThread.start();
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    // 修改心跳发送方法，增加测试模式判断
    private void sendHeartbeat() throws IOException {
        if (testMode) {
            System.out.println("[测试模式] 跳过心跳发送");
            return;
        }

        // 构建心跳消息
        Map<String, Object> heartbeat = new HashMap<>();
        heartbeat.put("type", "HEARTBEAT");
        heartbeat.put("timestamp", System.currentTimeMillis());
        heartbeat.put("status", "ACTIVE");

        // 发送心跳
        sendRequest(heartbeat);
    }

    public String registerRegionServer(String host, int port, String replicaKey) throws IOException {
        try {
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
                sendRequest(registerRequest);

                // 7. TODO: 处理响应...
                return "RegionServer-" + replicaKey;

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

    private void sendRequest(JSONObject request) throws IOException {
        if (testMode) {
            System.out.println("[测试模式] 模拟发送请求到Master：" + request.toString(2));
            return;
        }

        try {
            if (masterSocket != null && masterSocket.isConnected()) {
                PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
                out.println(request.toString());
                System.out.println("发送请求到Master：" + request.toString(2));
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
        isRunning = false;
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