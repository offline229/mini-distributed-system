package com.mds.region.handler;

import com.mds.region.handler.DBHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.json.JSONObject;

import com.mds.region.RegionServer;
import com.mds.region.constant.JsonFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);
    private static final int DEFAULT_CLIENT_PORT = 8000;
    private ServerSocket serverSocket;
    private ExecutorService threadPool;
    private DBHandler dbHandler;
    private static final int SOCKET_TIMEOUT = 30000; // 30秒超时
    private final RegionServer regionServer;

    public ClientHandler(RegionServer regionServer) {
        this.regionServer = regionServer;
    }

    public void start() throws IOException {
        int clientPort = regionServer.getClientPort() != -1 ? regionServer.getClientPort() : DEFAULT_CLIENT_PORT;
        serverSocket = new ServerSocket(clientPort);
        threadPool = Executors.newFixedThreadPool(8); // 固定线程池

        Thread serverThread = new Thread(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    threadPool.submit(() -> handleClientRequest(socket));
                } catch (IOException e) {
                    if (!serverSocket.isClosed()) {
                        System.err.println("ClientHandler 接收连接失败：" + e.getMessage());
                    }
                }
            }
        });
        serverThread.start();

        System.out.println("ClientHandler 已启动，监听端口：" + clientPort);
    }

    private void handleClientRequest(Socket socket) {
        PrintWriter writer = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            writer = new PrintWriter(socket.getOutputStream(), true);

            // 读取请求
            String requestStr = reader.readLine();
            if (requestStr == null || requestStr.isEmpty()) {
                throw new IOException("无效的请求");
            }

            logger.info("收到客户端请求: {}", requestStr);
            JSONObject request = new JSONObject(requestStr);

            // 从请求中获取SQL和参数
            String sql = request.getString("sql");
            Object[] params = request.has("params") ? request.getJSONArray("params").toList().toArray() : null;

            // 通过RegionServer处理请求
            Object result = regionServer.handleRequest(sql, params);

            // 构建响应
            JSONObject response = new JSONObject();
            response.put("status", "success");
            response.put("data", result != null ? result : JSONObject.NULL);
            response.put("message", "");

            writer.println(response.toString());

        } catch (Exception e) {
            logger.error("处理客户端请求失败: {}", e.getMessage());
            try {
                if (writer != null) {
                    JSONObject errorResponse = new JSONObject()
                            .put("status", "error")
                            .put("message", e.getMessage())
                            .put("data", JSONObject.NULL);
                    writer.println(errorResponse.toString());
                }
            } catch (Exception ex) {
                logger.error("发送错误响应失败: {}", ex.getMessage());
            }
        }
    }

    private void configureSocket(Socket socket) throws SocketException {
        socket.setSoTimeout(SOCKET_TIMEOUT);
        socket.setKeepAlive(true);
    }

    public void stop() throws IOException {
        if (serverSocket != null)
            serverSocket.close();
        if (threadPool != null)
            threadPool.shutdown();
        System.out.println("ClientHandler 已停止");
    }
}