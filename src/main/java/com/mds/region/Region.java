package com.mds.region;

import com.mds.region.handler.ClientHandler;
import com.mds.region.handler.MasterHandler;
import com.mds.region.handler.DBHandler;
import com.mds.region.handler.ZookeeperHandler;

public class Region {
    private MasterHandler masterHandler;
    private ClientHandler clientHandler;
    private DBHandler dbHandler;
    private ZookeeperHandler zkHandler;

    private String regionId;
    private String host;
    private int port;
    private volatile boolean isRunning;

    public Region(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() {
        try {
            // 1. 首先启动Master通信处理器
            masterHandler = new MasterHandler();
            masterHandler.start();

            // 2. 向Master注册并获取regionId
            this.regionId = masterHandler.registerWithMaster(host, port);
            if (regionId == null) {
                throw new RuntimeException("从Master获取Region ID失败");
            }

            // 3. 准备Region的元数据
            String regionData = String.format("host=%s,port=%d", host, port);

            // 4. 初始化并注册到ZooKeeper
            zkHandler = new ZookeeperHandler();
            zkHandler.init();
            zkHandler.registerRegion(regionId, regionData);

            // 5. 初始化数据库处理器
            dbHandler = new DBHandler();
            dbHandler.init();

            // 6. 启动客户端请求处理器
            clientHandler = new ClientHandler(dbHandler);
            clientHandler.start();

            isRunning = true;
            System.out.println("Region 启动成功！RegionId: " + regionId);
        } catch (Exception e) {
            System.err.println("Region 启动失败：" + e.getMessage());
            stop();
        }
    }

    public void stop() {
        try {
            System.out.println("开始停止 Region: " + regionId);

            // 停止 Client 和 Master 的处理器
            if (clientHandler != null)
                clientHandler.stop();
            if (masterHandler != null)
                masterHandler.stop();

            // 关闭数据库处理器
            if (dbHandler != null)
                dbHandler.close();

            // 注销 ZooKeeper 中的 Region 节点
            if (zkHandler != null)
                zkHandler.unregisterRegion(regionId);

            System.out.println("Region 停止成功！");
        } catch (Exception e) {
            System.err.println("Region 停止失败：" + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // 示例：启动一个Region节点
        String host = "localhost";
        int port = 8000;

        Region region = new Region(host, port);
        region.start();

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(region::stop));
    }
}