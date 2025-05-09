package com.mds.region;

import com.mds.common.model.RegionInfo;
import com.mds.common.util.ZookeeperUtil;
import com.mds.common.config.SystemConfig;
import com.mds.region.service.RegionService;
import com.mds.region.service.impl.RegionServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Region {
    private static final Logger logger = LoggerFactory.getLogger(Region.class);
    private final String regionId;
    private final String host;
    private final int port;
    private final String zkConnectString;
    private ZookeeperUtil zkUtil;
    private RegionService regionService;
    private boolean isRunning;

    public Region(String regionId, String host, int port, String zkConnectString) {
        this.regionId = regionId;
        this.host = host;
        this.port = port;
        this.zkConnectString = zkConnectString;
        this.isRunning = false;
    }

    public void start() {
        try {
            logger.info("开始启动Region: {}", regionId);
            
            // 初始化ZooKeeper连接
            zkUtil = new ZookeeperUtil();
            zkUtil.connect(zkConnectString, 5000);
            
            // 初始化Region服务
            regionService = new RegionServiceImpl(regionId);
            
            // 注册Region信息
            RegionInfo regionInfo = new RegionInfo();
            regionInfo.setRegionId(regionId);
            regionInfo.setHost(host);
            regionInfo.setPort(port);
            regionInfo.setStatus("ACTIVE");
            regionInfo.setLastHeartbeat(System.currentTimeMillis());
            
            if (!regionService.register(regionInfo)) {
                throw new RuntimeException("Region注册失败");
            }
            
            isRunning = true;
            logger.info("Region启动成功: {}", regionId);
        } catch (Exception e) {
            logger.error("Region启动失败: {}", e.getMessage(), e);
            throw new RuntimeException("Region启动失败", e);
        }
    }

    public void stop() {
        try {
            logger.info("开始停止Region: {}", regionId);
            if (regionService != null) {
                regionService.shutdown();
            }
            if (zkUtil != null) {
                zkUtil.close();
            }
            isRunning = false;
            logger.info("Region停止成功: {}", regionId);
        } catch (Exception e) {
            logger.error("Region停止失败: {}", e.getMessage(), e);
            throw new RuntimeException("Region停止失败", e);
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public RegionService getRegionService() {
        return regionService;
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