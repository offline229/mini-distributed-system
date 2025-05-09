package com.mds.region.handler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterHandler {
    private static final int MASTER_PORT = 9000;
    private ServerSocket serverSocket;
    private ExecutorService threadPool;

    public void start() throws IOException {
        serverSocket = new ServerSocket(MASTER_PORT);
        threadPool = Executors.newFixedThreadPool(4); // 固定线程池

        Thread serverThread = new Thread(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    threadPool.submit(() -> handleMasterRequest(socket));
                } catch (IOException e) {
                    if (!serverSocket.isClosed()) {
                        System.err.println("MasterHandler 接收连接失败：" + e.getMessage());
                    }
                }
            }
        });
        serverThread.start();

        System.out.println("MasterHandler 已启动，监听端口：" + MASTER_PORT);
    }

    private void handleMasterRequest(Socket socket) {
        // 处理来自 Master 的请求
        System.out.println("处理来自 Master 的请求...");
        // TODO: 实现具体的指令处理逻辑
    }

    public void stop() throws IOException {
        if (serverSocket != null) serverSocket.close();
        if (threadPool != null) threadPool.shutdown();
        System.out.println("MasterHandler 已停止");
    }
}