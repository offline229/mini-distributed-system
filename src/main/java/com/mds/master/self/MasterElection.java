package com.mds.master.self;

import com.mds.master.MasterDispatcher;
import com.mds.master.RegionWatcher;
import com.mds.master.ZKSyncManager;
import com.mds.master.MasterServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class MasterElection {
    private final CuratorFramework zkClient;
    private final LeaderSelector leaderSelector;
    private final String MASTER_PATH = "/master";     //选主路径
    private final String masterID;                    //主id
    private final AtomicBoolean isRunning;
    private CountDownLatch masterLatch;
    private volatile MasterServer masterServer;       // 使用volatile确保可见性
    private volatile RegionWatcher regionWatcher;     // 使用volatile确保可见性
    private volatile MasterDispatcher dispatcher;     // 添加dispatcher字段
    private static final int MASTER_PORT = 8000;      // Master服务端口

    public MasterElection(CuratorFramework zkClient, String masterID) {
        this.zkClient = zkClient;
        this.masterID = masterID;
        this.isRunning = new AtomicBoolean(true);

        this.leaderSelector = new LeaderSelector(
                zkClient,
                MASTER_PATH,
                new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception {
                        System.out.println("主节点选举成功！当前主节点 masterID: " + masterID);
                        masterLatch = new CountDownLatch(1);

                        try {
                            // 初始化各个组件
                            regionWatcher = new RegionWatcher(client);
                            ZKSyncManager zkSyncManager = new ZKSyncManager(client.getZookeeperClient().getZooKeeper());
                            MetaManager metaManager = new MetaManager(regionWatcher);
                            dispatcher = new MasterDispatcher(metaManager, regionWatcher);

                            // 启动Region监控
                            regionWatcher.startWatching();
                            System.out.println("RegionWatcher 启动成功");

                            // 启动Master服务器
                            masterServer = new MasterServer(MASTER_PORT, metaManager, zkSyncManager, regionWatcher, dispatcher);
                            Thread serverThread = new Thread(() -> {
                                try {
                                    masterServer.start();
                                } catch (Exception e) {
                                    System.err.println("MasterServer运行异常: " + e.getMessage());
                                    isRunning.set(false);
                                }
                            }, "MasterServer-Thread");
                            serverThread.start();
                            System.out.println("MasterServer 启动成功，监听端口: " + MASTER_PORT);

                            // 启动调度服务
                            dispatcher.start();
                            System.out.println("MasterDispatcher 启动成功");

                            // 等待直到被中断或主动退出
                            while (isRunning.get()) {
                                Thread.sleep(1000);
                                // 检查各组件状态
                                if (!regionWatcher.isRunning() || masterServer == null) {
                                    System.err.println("关键组件已停止，准备退出主节点");
                                    isRunning.set(false);
                                }
                            }

                        } catch (Exception e) {
                            System.err.println("主节点运行异常：" + masterID);
                            e.printStackTrace();
                        } finally {
                            cleanup();
                        }
                    }
                }
        );
        this.leaderSelector.autoRequeue();  //掉线则重新参选
    }

    private void cleanup() {
        try {
            System.out.println("开始清理资源...");
            
            if (dispatcher != null) {
                dispatcher.stop();
                System.out.println("MasterDispatcher已停止");
            }

            if (masterServer != null) {
                masterServer.stop();
                System.out.println("MasterServer已停止");
            }

            if (regionWatcher != null) {
                regionWatcher.stop();
                System.out.println("RegionWatcher已停止");
            }
        } catch (Exception e) {
            System.err.println("清理资源时发生错误: " + e.getMessage());
        } finally {
            masterLatch.countDown();
            System.out.println("主节点控制权释放！masterID: " + masterID);
        }
    }

    public void start() {
        System.out.println("开始选举！masterID: " + masterID);
        this.leaderSelector.start();
    }

    public void close() {
        System.out.println("正在关闭主节点...");
        isRunning.set(false);
        if (masterLatch != null) {
            try {
                masterLatch.await();  // 等待资源清理完成
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("等待资源清理时被中断");
            }
        }
        this.leaderSelector.close();
        System.out.println("主节点已关闭！masterID: " + masterID);
    }
}