package com.mds.region;

import com.mds.common.util.ZookeeperUtil;
import com.mds.region.communication.RegionServer;
import com.mds.region.processor.RegionProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Region {
    private static final Logger logger = LoggerFactory.getLogger(Region.class);
    
    private final String regionId;
    private final String host;
    private final int port;
    private final String zkConnectString;
    private final ZookeeperUtil zkUtil;
    private final RegionServer server;
    private final RegionProcessor processor;

    public Region(String regionId, String host, int port, String zkConnectString) {
        this.regionId = regionId;
        this.host = host;
        this.port = port;
        this.zkConnectString = zkConnectString;
        this.zkUtil = new ZookeeperUtil();
        this.server = new RegionServer(regionId, host, port, zkUtil);
        this.processor = new RegionProcessor(regionId);
    }

    public void start() throws Exception {
        logger.info("Starting Region: {}", regionId);
        
        // 连接ZooKeeper
        zkUtil.connect(zkConnectString, 5000);
        logger.info("Connected to ZooKeeper: {}", zkConnectString);
        
        // 启动处理器
        processor.start();
        logger.info("Region processor started");
        
        // 启动服务器
        server.start();
        logger.info("Region server started");
        
        logger.info("Region {} started successfully", regionId);
    }

    public void stop() {
        logger.info("Stopping Region: {}", regionId);
        
        // 停止服务器
        server.stop();
        logger.info("Region server stopped");
        
        // 停止处理器
        processor.stop();
        logger.info("Region processor stopped");
        
        // 关闭ZooKeeper连接
        zkUtil.close();
        logger.info("ZooKeeper connection closed");
        
        logger.info("Region {} stopped successfully", regionId);
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: Region <regionId> <host> <port> <zkConnectString>");
            System.exit(1);
        }

        String regionId = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        String zkConnectString = args[3];

        Region region = new Region(regionId, host, port, zkConnectString);
        
        try {
            region.start();
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(region::stop));
            
            // 保持主线程运行
            Thread.currentThread().join();
        } catch (Exception e) {
            logger.error("Error running Region", e);
            System.exit(1);
        }
    }
} 