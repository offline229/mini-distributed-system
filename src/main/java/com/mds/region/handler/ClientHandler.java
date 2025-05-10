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
import com.mds.region.constant.JsonFormat;

public class ClientHandler {
    private static final int CLIENT_PORT = 8000;
    private ServerSocket serverSocket;
    private ExecutorService threadPool;
    private DBHandler dbHandler;
    private static final int SOCKET_TIMEOUT = 30000; // 30秒超时

    public void start() throws IOException {
        serverSocket = new ServerSocket(CLIENT_PORT);
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

        System.out.println("ClientHandler 已启动，监听端口：" + CLIENT_PORT);
    }

    private void handleClientRequest(Socket socket) {
        try (socket;
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            // 1. 读取客户端请求
            String request = reader.readLine();
            System.out.println("收到客户端请求: " + request);

            // 2. 解析请求
            JSONObject requestJson = new JSONObject(request);
            String operation = requestJson.getString(JsonFormat.FIELD_OPERATION);
            String sql = requestJson.getString(JsonFormat.FIELD_SQL);
            Object[] params = null;
            if (requestJson.has(JsonFormat.FIELD_PARAMS)) {
                params = requestJson.getJSONArray(JsonFormat.FIELD_PARAMS).toList().toArray();
            }

            // 3. 执行数据库操作
            Object result = null;
            try {
                result = dbHandler.execute(sql, params);

                // 4. 返回成功结果
                JSONObject response = new JSONObject();
                response.put(JsonFormat.FIELD_STATUS, JsonFormat.STATUS_SUCCESS);
                response.put(JsonFormat.FIELD_DATA, result);
                response.put(JsonFormat.FIELD_MESSAGE, "");
                writer.println(response.toString());

            } catch (Exception e) {
                // 5. 返回错误结果
                JSONObject errorResponse = new JSONObject();
                errorResponse.put(JsonFormat.FIELD_STATUS, JsonFormat.STATUS_ERROR);
                errorResponse.put(JsonFormat.FIELD_DATA, JSONObject.NULL);
                errorResponse.put(JsonFormat.FIELD_MESSAGE, e.getMessage());
                writer.println(errorResponse.toString());
            }

        } catch (Exception e) {
            System.err.println("处理客户端请求失败: " + e.getMessage());
        }
    }

    private void configureSocket(Socket socket) throws SocketException {
        socket.setSoTimeout(SOCKET_TIMEOUT);
        socket.setKeepAlive(true);
    }

    public ClientHandler(DBHandler dbHandler) {
        this.dbHandler = dbHandler;
    }

    public void stop() throws IOException {
        if (serverSocket != null)
            serverSocket.close();
        if (threadPool != null)
            threadPool.shutdown();
        System.out.println("ClientHandler 已停止");
    }
}