package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionServerInfo;
import com.mds.common.RegionServerInfo.HostPortStatus;
import com.mds.master.self.MetaManager;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterServer {
    private final int port;                         // 主节点监听端口

    private ServerSocket serverSocket;              //Socket连接
    private final ExecutorService threadPool;       //线程池，用于处理并发请求
    private final ObjectMapper mapper = new ObjectMapper();     // 用于序列化/反序列化 JSON

    private final MetaManager metaManager;
    private final ZKSyncManager zkSyncManager;
    private final RegionWatcher regionWatcher;
    private final MasterDispatcher masterDispatcher;

    public MasterServer(int port, MetaManager metaManager, ZKSyncManager zkSyncManager, RegionWatcher regionWatcher, MasterDispatcher masterDispatcher) {
        this.port = port;
        this.metaManager = metaManager;
        this.metaManager.init();                    // 初始化 MetaManager——新建表
        this.zkSyncManager = zkSyncManager;
        this.regionWatcher = regionWatcher;
        this.masterDispatcher = masterDispatcher;

        this.threadPool = Executors.newCachedThreadPool();      // 创建一个可缓存的线程池
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);  //绑定端口
            System.out.println("MasterServer 启动监听端口 " + port);

            // 启动 RegionWatcher 来监控 ZK 中的 Region 节点
            if (regionWatcher != null) {
                regionWatcher.startWatching();
                System.out.println("RegionWatcher 已启动");
            }

            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();              // 接收到新的连接
                System.out.println("接收到新的连接: " + socket.getRemoteSocketAddress());
                threadPool.submit(() -> handleConnection(socket));  // 提交处理任务到线程池
            }
        } catch (IOException e) {
            System.err.println("启动 MasterServer 失败: " + e.getMessage());
            stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            if (regionWatcher != null) {
                regionWatcher.stop();
                System.out.println("RegionWatcher 已停止");
            }
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            threadPool.shutdownNow();   // 强制关闭线程池，释放资源
            System.out.println("MasterServer 已关闭");
        } catch (IOException e) {
            System.err.println("关闭 MasterServer 出错: " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 解析请求并返回响应
    private void handleConnection(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String requestLine;
            while ((requestLine = in.readLine()) != null) {
                // 解析请求
                Map<String, Object> request = mapper.readValue(requestLine, Map.class);
                String type = (String) request.get("type");

                if ("REGISTER".equalsIgnoreCase(type)) {
                    handleRegionRegistration(request, out); // 处理 RegionServer 注册请求
                } else if ("HEARTBEAT".equalsIgnoreCase(type)) {
                    handleHeartbeat(request, out);          // 处理 RegionServer 心跳请求
                } else if ("SQL".equalsIgnoreCase(type)) {
                    handleClientSqlRequest(request, out);   // 处理客户端 SQL 请求
                } else {
                    sendErrorResponse(out, "Unknown request type: " + type, null);
                }
            }
        } catch (IOException e) {
            System.err.println("处理 socket 请求失败: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("关闭 socket 出错: " + e.getMessage());
            }
        }
    }

    // 处理 RegionServer 注册请求
    private void handleRegionRegistration(Map<String, Object> request, PrintWriter out) {
        try {
            String host = (String) request.get("host");
            int port = (int) request.get("port");
            String replicaKey = (String) request.get("replicaKey");

            System.out.println("[MasterServer] 收到 RegionServer 注册请求: host=" + host + ", port=" + port + ", replicaKey=" + replicaKey);

            // 检查请求参数，是否是新的RegionServer--备份数据库
            RegionServerInfo existingRegionServer = regionWatcher.findRegionServerByReplicaKey(replicaKey);

            String regionserverId;
            if (existingRegionServer != null) { // 已存在的 RegionServer
                System.out.println("[MasterServer] 已存在的 RegionServer: " + existingRegionServer.getRegionserverID());
                // 检查是否已存在相同的 host:port
                boolean hostPortExists = existingRegionServer.getHostsPortsStatusList().stream()
                    .anyMatch(hp -> hp.getHost().equals(host) && hp.getPort() == port);

                if (!hostPortExists) {          // 新的 host:port
                    // 添加新的 host:port
                    existingRegionServer.getHostsPortsStatusList().add(
                        new HostPortStatus(host, port, "active", 0, System.currentTimeMillis())
                    );
                    // 更新 RegionServer 信息
                    zkSyncManager.updateRegionInfo(existingRegionServer);
                    metaManager.updateRegionInfo(existingRegionServer);
                }                               // 旧的不做变化
                // 更新 RegionServer 的心跳时间
                regionserverId = existingRegionServer.getRegionserverID();
            } else {                            // 新的 RegionServer
                regionserverId = UUID.randomUUID().toString();
                List<HostPortStatus> hostPorts = new ArrayList<>();
                hostPorts.add(new HostPortStatus(host, port, "active", 0, System.currentTimeMillis()));

                // 注册新的 RegionServerInfo
                RegionServerInfo newRegionServer = new RegionServerInfo(
                    regionserverId,
                    hostPorts,
                    replicaKey,
                    System.currentTimeMillis()
                );

                // 同步到 ZK 和 MySQL
                zkSyncManager.registerRegion(newRegionServer);
                metaManager.saveRegionInfo(newRegionServer);
            }

            // 返回注册成功的响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "ok");
            response.put("regionserverId", regionserverId);
            sendJsonResponse(out, response);

            System.out.println("[MasterServer] RegionServer 注册成功: " + regionserverId);
        } catch (Exception e) {
            System.err.println("[MasterServer] 处理 Region 注册失败: " + e.getMessage());
            sendErrorResponse(out, "注册失败: " + e.getMessage(), null);
        }
    }

    // 处理 RegionServer 心跳请求
    public void handleHeartbeat(Map<String, Object> request, PrintWriter out) {
        try {
            String regionserverId = (String) request.get("regionserverId");
            String host = (String) request.get("host");
            int port = (int) request.get("port");
            int connections = (int) request.getOrDefault("connections", 0);

            // 在 RegionWatcher 中查找 RegionServer
            RegionServerInfo region = regionWatcher.getRegionById(regionserverId);
            if (region != null) {   // RegionServer 存在
                // 更新匹配的 host:port 的连接数和心跳时间
                region.getHostsPortsStatusList().stream()
                    .filter(hp -> hp.getHost().equals(host) && hp.getPort() == port)
                    .forEach(hp -> {
                        hp.setConnections(connections);
                        hp.setLastHeartbeatTime(System.currentTimeMillis());
                    });

                // 更新 RegionServer 信息
                zkSyncManager.updateRegionInfo(region);
                metaManager.updateRegionInfo(region);

                // 返回心跳成功的响应
                Map<String, Object> response = new HashMap<>();
                response.put("status", "ok");
                response.put("message", "Heartbeat received");
                sendJsonResponse(out, response);

                System.out.println("收到 RegionServer 心跳: " + regionserverId + " from " + host + ":" + port);
            } else {                // RegionServer 不存在
                sendErrorResponse(out, "未知的 RegionServer: " + regionserverId, null);
            }
        } catch (Exception e) {
            System.err.println("处理心跳失败: " + e.getMessage());
            sendErrorResponse(out, "处理心跳失败: " + e.getMessage(), null);
        }
    }

    // 处理客户端 SQL 请求
    private void handleClientSqlRequest(Map<String, Object> request, PrintWriter out) {
        try {
            String sql = (String) request.get("sql");
            // 检查 SQL 是否为空
            if (sql == null || sql.trim().isEmpty()) {
                sendErrorResponse(out, "SQL 不能为空", null);
                return;
            }

            System.out.println("收到客户端 SQL 请求: " + sql);

            // 调用 MasterDispatcher 处理 SQL
            Map<String, Object> dispatchResult = masterDispatcher.dispatch(sql);
            // 检查处理结果
            String responseType = (String) dispatchResult.get("type");
            // 根据处理结果返回响应
            Map<String, Object> response = new HashMap<>();

            if (MasterDispatcher.RESPONSE_TYPE_DML_REDIRECT.equals(responseType)) {
                // DML 请求，返回目标 RegionServer
                response.put("status", "ok");
                response.put("type", "DML_REDIRECT");
                response.put("regionId", dispatchResult.get("regionId"));
                response.put("host", dispatchResult.get("host"));
                response.put("port", dispatchResult.get("port"));
                 
                // 打印发送给客户端的响应
                System.out.println("DML请求发送给客户端的响应: " + response);

                sendJsonResponse(out, response);

            } else if (MasterDispatcher.RESPONSE_TYPE_DDL_RESULT.equals(responseType)) {
                // DDL 请求，返回目标 RegionServer
                response.put("status", "ok");
                response.put("type", "DDL_RESULT");
                response.put("regionId", dispatchResult.get("regionId"));
                response.put("host", dispatchResult.get("host"));
                response.put("port", dispatchResult.get("port"));
                
                // 打印发送给客户端的响应
                System.out.println("DDL请求发送给客户端的响应: " + response);

                sendJsonResponse(out, response);
            } else if (MasterDispatcher.RESPONSE_TYPE_ERROR.equals(responseType)) {
                // 错误处理
                sendErrorResponse(out, (String) dispatchResult.get("message"), null);
            } else {
                sendErrorResponse(out, "未知的响应类型: " + responseType, null);
            }
        } catch (Exception e) {
            System.err.println("处理客户端 SQL 请求失败: " + e.getMessage());
            sendErrorResponse(out, "SQL 请求处理失败: " + e.getMessage(), null);
        }
    }

    // 发送 JSON 响应
    private void sendJsonResponse(PrintWriter out, Map<String, Object> responseMap) {
        try {
            // 将 Map 转换为 JSON 字符串
            String jsonResponse = mapper.writeValueAsString(responseMap);
            out.println(jsonResponse);
        } catch (IOException e) {
            System.err.println("发送 JSON 响应失败: " + e.getMessage());
        }
    }

    // 发送错误响应
    private void sendErrorResponse(PrintWriter out, String message, Map<String, Object> details) {
        // 构建错误响应
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("status", "error");
        errorResponse.put("message", message);
        if (details != null) {
            errorResponse.put("details", details);
        }
        sendJsonResponse(out, errorResponse);
    }
}