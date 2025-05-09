package com.mds.region.handler;

import com.mds.region.handler.DBHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientHandler {
    private static final int CLIENT_PORT = 8000;
    private ServerSocket serverSocket;
    private ExecutorService threadPool;
    private DBHandler dbHandler;

    public ClientHandler(DBHandler dbHandler) {
        this.dbHandler = dbHandler;
    }

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
        // 处理来自 Client 的请求
        System.out.println("处理来自 Client 的请求...");
        // TODO: 实现具体的请求处理逻辑
    }

    public void stop() throws IOException {
        if (serverSocket != null) serverSocket.close();
        if (threadPool != null) threadPool.shutdown();
        System.out.println("ClientHandler 已停止");
    }
}