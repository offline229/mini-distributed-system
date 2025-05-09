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

    private String regionId; // Region 的唯一标识
    private String regionData; // Region 的元数据信息

    public Region(String regionId, String regionData) {
        this.regionId = regionId;
        this.regionData = regionData;
    }

    public void start() {
        try {
            // 初始化 ZooKeeper 处理器并注册 Region 节点
            zkHandler = new ZookeeperHandler();
            zkHandler.init();
            zkHandler.registerRegion(regionId, regionData);

            // 初始化数据库处理器（仅验证连接）
            dbHandler = new DBHandler();
            dbHandler.init();

            // 启动 Master 和 Client 的处理器
            masterHandler = new MasterHandler();
            masterHandler.start();

            clientHandler = new ClientHandler(dbHandler);
            clientHandler.start();

            System.out.println("Region 启动成功！");
        } catch (Exception e) {
            System.err.println("Region 启动失败：" + e.getMessage());
            stop();
        }
    }

    public void stop() {
        try {
            System.out.println("开始停止 Region: " + regionId);

            // 停止 Client 和 Master 的处理器
            if (clientHandler != null) clientHandler.stop();
            if (masterHandler != null) masterHandler.stop();

            // 关闭数据库处理器
            if (dbHandler != null) dbHandler.close();

            // 注销 ZooKeeper 中的 Region 节点
            if (zkHandler != null) zkHandler.unregisterRegion(regionId);

            System.out.println("Region 停止成功！");
        } catch (Exception e) {
            System.err.println("Region 停止失败：" + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // 示例：创建一个 Region 节点
        String regionId = "region-1";
        String regionData = "host=127.0.0.1,port=8000";

        Region region = new Region(regionId, regionData);
        region.start();

        // 添加钩子以便优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(region::stop));
    }
}