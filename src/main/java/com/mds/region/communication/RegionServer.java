package com.mds.region.communication;

import com.mds.common.model.RegionInfo;
import com.mds.common.util.ZookeeperUtil;
import com.mds.common.config.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class RegionServer {
    private static final Logger logger = LoggerFactory.getLogger(RegionServer.class);
    
    private final String regionId;
    private final String host;
    private final int port;
    private final ZookeeperUtil zkUtil;
    private final ExecutorService threadPool;
    private final AtomicBoolean isRunning;
    private ServerSocket serverSocket;
    private Thread serverThread;

    public RegionServer(String regionId, String host, int port, ZookeeperUtil zkUtil) {
        this.regionId = regionId;
        this.host = host;
        this.port = port;
        this.zkUtil = zkUtil;
        this.threadPool = Executors.newFixedThreadPool(10);
        this.isRunning = new AtomicBoolean(false);
    }

    public void start() throws IOException {
        if (isRunning.compareAndSet(false, true)) {
            // 注册到ZooKeeper
            registerToZooKeeper();
            
            // 启动服务器
            serverSocket = new ServerSocket(port);
            serverThread = new Thread(this::acceptConnections);
            serverThread.start();
            
            logger.info("Region server started on {}:{}", host, port);
        }
    }

    private void registerToZooKeeper() {
        try {
            String regionPath = SystemConfig.ZK_REGION_PATH + "/" + regionId;
            RegionInfo regionInfo = new RegionInfo();
            regionInfo.setRegionId(regionId);
            regionInfo.setHost(host);
            regionInfo.setPort(port);
            regionInfo.setStatus("ACTIVE");
            regionInfo.setCreateTime(System.currentTimeMillis());
            
            // 创建临时节点，当Region断开连接时自动删除
            zkUtil.createPath(regionPath, org.apache.zookeeper.CreateMode.EPHEMERAL);
            zkUtil.setData(regionPath, regionInfo.toString().getBytes());
            
            logger.info("Region registered to ZooKeeper: {}", regionPath);
        } catch (Exception e) {
            logger.error("Failed to register region to ZooKeeper", e);
            throw new RuntimeException("Failed to register region to ZooKeeper", e);
        }
    }

    private void acceptConnections() {
        try {
            while (isRunning.get() && !Thread.currentThread().isInterrupted()) {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(() -> handleClientConnection(clientSocket));
            }
        } catch (IOException e) {
            if (isRunning.get()) {
                logger.error("Error accepting client connections", e);
            }
        }
    }

    private void handleClientConnection(Socket clientSocket) {
        try {
            // TODO: 实现具体的请求处理逻辑
            // 这里可以添加请求解析、路由到对应的处理模块等逻辑
        } catch (Exception e) {
            logger.error("Error handling client connection", e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                logger.error("Error closing client socket", e);
            }
        }
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            // 关闭服务器线程
            if (serverThread != null) {
                serverThread.interrupt();
            }
            
            // 关闭服务器Socket
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    logger.error("Error closing server socket", e);
                }
            }
            
            // 关闭线程池
            threadPool.shutdown();
            
            // 从ZooKeeper注销
            try {
                String regionPath = SystemConfig.ZK_REGION_PATH + "/" + regionId;
                zkUtil.delete(regionPath);
                logger.info("Region unregistered from ZooKeeper: {}", regionPath);
            } catch (Exception e) {
                logger.error("Error unregistering region from ZooKeeper", e);
            }
            
            logger.info("Region server stopped");
        }
    }

    public boolean isRunning() {
        return isRunning.get();
    }
} 