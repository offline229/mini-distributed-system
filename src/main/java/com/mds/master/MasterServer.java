package com.mds.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.RegionInfo;
import com.mds.master.self.MetaManager;
import com.mds.master.ZKSyncManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * MasterServer 用于接收来自 Region 和 Client 的 socket 请求，如注册、心跳、SQL等。
 */
public class MasterServer {
    private static final Logger logger = LoggerFactory.getLogger(MasterServer.class);

    private final int port;
    private final MetaManager metaManager;
    private final ZKSyncManager zkSyncManager;
    private final MasterDispatcher masterDispatcher;
    private final RegionWatcher regionWatcher;
    private ServerSocket serverSocket;
    private final ExecutorService threadPool;

    private final ObjectMapper mapper = new ObjectMapper(); // 用于序列化/反序列化 JSON

    public MasterServer(int port, MetaManager metaManager, ZKSyncManager zkSyncManager, MasterDispatcher masterDispatcher, RegionWatcher regionWatcher) {
        this.port = port;
        this.metaManager = metaManager;
        this.zkSyncManager = zkSyncManager;
        this.masterDispatcher = masterDispatcher;
        this.regionWatcher = regionWatcher;
        this.threadPool = Executors.newCachedThreadPool();
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            logger.info("MasterServer 启动监听端口 {}", port);

            // 启动 RegionWatcher 来监控 ZK 中的 Region 节点
            if (regionWatcher != null) {
                try {
                    regionWatcher.startWatching();
                    logger.info("RegionWatcher 已启动");
                } catch (Exception e) {
                    logger.error("启动 RegionWatcher 失败", e);
                }
            }

            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                logger.info("接收到新的连接: {}", socket.getRemoteSocketAddress());
                threadPool.submit(() -> handleConnection(socket));
            }
        } catch (IOException e) {
            logger.error("启动 MasterServer 失败", e);
            stop();
        }
    }

    public void stop() {
        try {
            if (regionWatcher != null) {
                try {
                    regionWatcher.stop();
                    logger.info("RegionWatcher 已停止");
                } catch (Exception e) {
                    logger.error("停止 RegionWatcher 失败", e);
                }
            }
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            threadPool.shutdownNow(); // 强制关闭线程池
            logger.info("MasterServer 已关闭");
        } catch (IOException e) {
            logger.error("关闭 MasterServer 出错", e);
        }
    }

    private void handleConnection(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true) // 使用 PrintWriter 并启用 autoFlush
        ) {
            String requestLine;
            while ((requestLine = in.readLine()) != null) {
                Map<String, Object> request = mapper.readValue(requestLine, Map.class);
                String type = (String) request.get("type");

                if ("REGISTER".equalsIgnoreCase(type)) {
                    handleRegionRegistration(request, out);
                } else if ("HEARTBEAT".equalsIgnoreCase(type)) {
                    handleHeartbeat(request, out);
                } else if ("SQL".equalsIgnoreCase(type)) {
                    handleClientSqlRequest(request, out);
                } else {
                    logger.warn("未知请求类型: {}", type);
                    sendErrorResponse(out, "Unknown request type: " + type, null);                }
            }
        } catch (IOException e) {
            logger.error("处理 socket 请求失败", e);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                logger.warn("关闭 socket 出错", e);
            }
        }
    }

    private void handleRegionRegistration(Map<String, Object> request, PrintWriter out) {
        try {
            String host = (String) request.get("host");
            int port = (int) request.get("port");

            RegionInfo regionInfo = new RegionInfo();
            String regionId = UUID.randomUUID().toString().replace("-", "");
            regionInfo.setRegionId(regionId);
            regionInfo.setHost(host);
            regionInfo.setPort(port);
            regionInfo.setLoad(0);
            regionInfo.setCreateTime(System.currentTimeMillis());

            // 1. 保存元数据
            metaManager.saveRegionInfo(regionInfo);

            // 2. 注册到 ZooKeeper
            zkSyncManager.registerRegion(regionInfo);

            // 3. 返回响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "ok");
            response.put("regionId", regionId);
            sendJsonResponse(out, response);

            logger.info("Region [{}] 注册成功: {}:{}", regionId, host, port);
        } catch (Exception e) {
            logger.error("处理 Region 注册失败", e);
            sendErrorResponse(out, "注册失败: " + e.getMessage(), null);        }
    }

    private void handleHeartbeat(Map<String, Object> request, PrintWriter out) {
        try {
            String regionId = (String) request.get("regionId");
            if (regionId == null || regionId.isEmpty()) {
                sendErrorResponse(out, "心跳失败: regionId 不能为空", null);
                return;
            }
            // 心跳通常会携带负载信息
            int load = 0; // 默认为0
            if (request.containsKey("load") && request.get("load") instanceof Number) {
                load = ((Number) request.get("load")).intValue();
            }

            long timestamp = System.currentTimeMillis();
            if (request.containsKey("timestamp") && request.get("timestamp") instanceof Number) {
                timestamp = ((Number) request.get("timestamp")).longValue();
            }


            // 1. 更新 Master 内存中 RegionWatcher 的 RegionInfo 负载信息
            RegionInfo regionToUpdate = regionWatcher.getRegionById(regionId);
            if (regionToUpdate != null) {
                regionToUpdate.setLoad(load); // 更新内存中的负载，心跳的主要作用是报告存活和负载
                // 将此更新也同步到 MetaManager (MySQL) 和 ZKSyncManager (ZK 节点数据)
                 MetaManager.updateRegionStatus(regionId, load);
                 zkSyncManager.updateRegionInfo(regionToUpdate); // 更新 ZK 数据
                logger.info("收到 Region [{}] 的心跳: {}, 负载: {}", regionId, timestamp, load);
            } else {
                logger.warn("收到未知 Region [{}] 的心跳", regionId);
                // 可以选择是否报错，或者尝试重新注册该 Region
                sendErrorResponse(out, "心跳失败: 未知的 RegionId " + regionId, null);
                return;
            }

            Map<String, Object> response = new HashMap<>();
            response.put("status", "ok");
            response.put("message", "心跳已收到");
            sendJsonResponse(out, response);

        } catch (Exception e) {
            logger.error("处理心跳失败", e);
            sendErrorResponse(out, "心跳处理失败: " + e.getMessage(), null);
        }
    }

    // 处理来自客户端的 SQL 请求
    private void handleClientSqlRequest(Map<String, Object> request, PrintWriter out) {
        try {
            String sql = (String) request.get("sql");
            if (sql == null || sql.trim().isEmpty()) {
                sendErrorResponse(out, "SQL 不能为空", null);
                return;
            }
            logger.info("收到客户端 SQL 请求: {}", sql);

            // 调用 dispatcher 处理 SQL
            Map<String, Object> dispatchResult = masterDispatcher.dispatch(sql);
            String responseType = (String) dispatchResult.get("type");

            Map<String, Object> clientResponse = new HashMap<>();

            if (MasterDispatcher.RESPONSE_TYPE_DDL_RESULT.equals(responseType)) {
                // DDL 结果
                clientResponse.put("status", "ok"); // Assuming DDL processing in MasterDispatcher indicates overall status
                clientResponse.put("type", "DDL_RESULT");
                clientResponse.put("result", dispatchResult.get("result")); // DDL 广播结果
                sendJsonResponse(out, clientResponse);

            } else if (MasterDispatcher.RESPONSE_TYPE_DML_REDIRECT.equals(responseType)) {
                // DML 路由信息，返回 RegionInfo 列表给客户端
                @SuppressWarnings("unchecked")
                List<RegionInfo> targetRegions = (List<RegionInfo>) dispatchResult.get("regions");

                if (targetRegions != null && !targetRegions.isEmpty()) {
                    // 构建包含 RegionServer 地址列表的响应
                    List<Map<String, Object>> regionAddresses = new ArrayList<>();
                    for (RegionInfo region : targetRegions) {
                        Map<String, Object> regionAddr = new HashMap<>();
                        regionAddr.put("regionId", region.getRegionId());
                        regionAddr.put("host", region.getHost());
                        regionAddr.put("port", region.getPort()); // 这里是客户端直连的端口
                        regionAddresses.add(regionAddr);
                    }
                    clientResponse.put("status", "redirect"); // 定义一个重定向状态
                    clientResponse.put("message", "请直连以下 Region Server");
                    clientResponse.put("regions", regionAddresses);
                    sendJsonResponse(out, clientResponse);

                } else {
                    // 未找到合适的 Region Server
                    sendErrorResponse(out, "未找到负责处理该请求的 Region Server", null);
                }

            } else if (MasterDispatcher.RESPONSE_TYPE_ERROR.equals(responseType)) {
                // Dispatcher 处理出错
                sendErrorResponse(out, (String) dispatchResult.get("message"), null);

            } else {
                // 未知的 Dispatcher 响应类型
                logger.error("收到未知的 Dispatcher 响应类型: {}", responseType);
                sendErrorResponse(out, "内部错误: 未知的调度器响应", null);
            }

        } catch (Exception e) {
            logger.error("处理客户端 SQL 请求失败", e);
            sendErrorResponse(out, "SQL 请求处理失败: " + e.getMessage(), null);
        }
    }

    // 使用 PrintWriter 发送 JSON 响应 (Map)
    private void sendJsonResponse(PrintWriter out, Map<String, Object> responseMap) {
        try {
            String jsonResponse = mapper.writeValueAsString(responseMap);
            out.println(jsonResponse); // println 会自动添加换行符并 flush (如果 PrintWriter autoFlush 为 true)
            logger.debug("发送响应: {}", jsonResponse);
        } catch (IOException e) {
            logger.error("发送JSON响应失败", e);
        }
    }

    // 使用 PrintWriter 发送错误响应 (Map)
    private void sendErrorResponse(PrintWriter out, String message, Map<String, Object> details) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("status", "error");
        errorResponse.put("message", message);
        if (details != null && !details.isEmpty()) {
            errorResponse.put("details", details);
        }
        sendJsonResponse(out, errorResponse);
    }
}

