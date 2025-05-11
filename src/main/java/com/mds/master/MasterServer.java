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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * MasterServer 用于接收来自 Region 的 socket 请求，如注册、心跳等。
 */
public class MasterServer {
    private static final Logger logger = LoggerFactory.getLogger(MasterServer.class);

    private final int port;
    private final MetaManager metaManager;
    private final ZKSyncManager zkSyncManager;
    private ServerSocket serverSocket;
    private final ExecutorService threadPool;

    public MasterServer(int port, MetaManager metaManager, ZKSyncManager zkSyncManager) {
        this.port = port;
        this.metaManager = metaManager;
        this.zkSyncManager = zkSyncManager;
        this.threadPool = Executors.newCachedThreadPool();
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            logger.info("MasterServer 启动监听端口 {}", port);

            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                threadPool.submit(() -> handleConnection(socket));
            }
        } catch (IOException e) {
            logger.error("启动 MasterServer 失败", e);
        }
    }

    public void stop() {
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
            threadPool.shutdownNow();
            logger.info("MasterServer 已关闭");
        } catch (IOException e) {
            logger.error("关闭 MasterServer 出错", e);
        }
    }

    private void handleConnection(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))
        ) {
            String requestLine = in.readLine();
            if (requestLine == null || requestLine.isEmpty()) return;

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> request = mapper.readValue(requestLine, Map.class);
            String type = (String) request.get("type");

            if ("REGISTER".equalsIgnoreCase(type)) {
                handleRegionRegistration(request, out);
            } else if ("HEARTBEAT".equalsIgnoreCase(type)) {
                handleHeartbeat(request); // TODO: 实现 Region 心跳更新逻辑
            } else {
                logger.warn("未知请求类型: {}", type);
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

    private void handleRegionRegistration(Map<String, Object> request, BufferedWriter out) {
        try {
            String host = (String) request.get("host");
            int port = (int) request.get("port");

            RegionInfo regionInfo = new RegionInfo();
            String regionId = UUID.randomUUID().toString().replace("-", "");
            regionInfo.setRegionId(regionId);
            regionInfo.setHost(host);
            regionInfo.setPort(port);
            regionInfo.setLoad(0);

            // 持久化元数据
            metaManager.saveRegionInfo(regionInfo);

            // 注册到 ZooKeeper
            zkSyncManager.registerRegion(regionInfo);

            // 构造响应
            Map<String, Object> response = new HashMap<>();
            response.put("regionId", regionId);

            ObjectMapper mapper = new ObjectMapper();
            String responseJson = mapper.writeValueAsString(response);
            out.write(responseJson);
            out.newLine();
            out.flush();

            logger.info("Region [{}] 注册成功: {}:{}", regionId, host, port);
        } catch (Exception e) {
            logger.error("处理 Region 注册失败", e);
        }
    }

    private void handleHeartbeat(Map<String, Object> request) {
        String regionId = (String) request.get("regionId");
        long timestamp = (long) request.get("timestamp");

        // TODO: 更新该 Region 的状态或负载等
        logger.info("收到 Region [{}] 的心跳: {}", regionId, timestamp);
    }
}
