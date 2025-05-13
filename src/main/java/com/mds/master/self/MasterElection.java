package com.mds.master.self;

import com.mds.master.MasterDispatcher;
import com.mds.master.RegionWatcher;
import com.mds.master.ZKSyncManager;
import com.mds.master.MasterServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class MasterElection {
    private static final String MASTER_BASE_PATH = "/master";          // 基础路径
    private static final String ACTIVE_PATH = MASTER_BASE_PATH + "/active";    // 活跃主节点路径
    private static final String STANDBY_PATH = MASTER_BASE_PATH + "/standby";  // 备选节点路径
    private static final int MASTER_PORT = 8000;      // Master服务端口

    private final CuratorFramework zkClient;
    private final LeaderSelector leaderSelector;
    private final String masterID;
    private final AtomicBoolean isRunning;
    private CountDownLatch masterLatch;
    private volatile MasterServer masterServer;
    private volatile RegionWatcher regionWatcher;
    private volatile MasterDispatcher dispatcher;
    private volatile MetaManager metaManager;
    private volatile ZKSyncManager zkSyncManager;

    public MasterElection(CuratorFramework zkClient, String masterID) {
        this.zkClient = zkClient;
        this.masterID = masterID;
        this.isRunning = new AtomicBoolean(true);

        // 初始化ZK路径
        initializePaths();
        
        // 注册为备选节点
        registerAsStandby();

        this.leaderSelector = new LeaderSelector(
                zkClient,
                MASTER_BASE_PATH,
                new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception {
                        System.out.println("主节点选举成功！当前主节点 masterID: " + masterID);
                        masterLatch = new CountDownLatch(1);

                        try {
                            // 转换为活跃节点
                            promoteToActive();

                            // 初始化各个组件
                            initializeComponents(client);

                            // 启动所有服务
                            startServices();

                            // 等待直到被中断或主动退出
                            waitForInterruption();

                        } catch (Exception e) {
                            System.err.println("主节点运行异常：" + masterID);
                            e.printStackTrace();
                        } finally {
                            // 降级为备选节点
                            demoteToStandby();
                            cleanup();
                        }
                    }
                }
        );
        this.leaderSelector.autoRequeue();  // 掉线则重新参选
    }

    private void initializePaths() {
        try {
            ensurePath(MASTER_BASE_PATH);
            ensurePath(ACTIVE_PATH);
            ensurePath(STANDBY_PATH);
        } catch (Exception e) {
            throw new RuntimeException("初始化ZK路径失败", e);
        }
    }

    private void ensurePath(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);
            System.out.println("创建路径: " + path);
        }
    }

    private void registerAsStandby() {
        try {
            String standbyPath = STANDBY_PATH + "/" + masterID;
            zkClient.create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(standbyPath);
            System.out.println("注册为备选节点: " + standbyPath);
        } catch (Exception e) {
            throw new RuntimeException("注册备选节点失败", e);
        }
    }

    private void promoteToActive() throws Exception {
        // 删除备选节点
        zkClient.delete().forPath(STANDBY_PATH + "/" + masterID);
        
        // 创建活跃节点
        String activePath = ACTIVE_PATH + "/" + masterID;
        zkClient.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(activePath);
        System.out.println("升级为活跃主节点: " + activePath);
    }

    private void demoteToStandby() {
        try {
            // 删除活跃节点
            zkClient.delete().forPath(ACTIVE_PATH + "/" + masterID);
            
            // 重新注册为备选节点
            registerAsStandby();
            System.out.println("降级为备选节点完成");
        } catch (Exception e) {
            System.err.println("节点状态转换失败: " + e.getMessage());
        }
    }

    private void initializeComponents(CuratorFramework client) throws Exception {
        regionWatcher = new RegionWatcher(client);
        metaManager = new MetaManager(regionWatcher);
        zkSyncManager = new ZKSyncManager(client.getZookeeperClient().getZooKeeper());
        dispatcher = new MasterDispatcher(metaManager, regionWatcher);
    }

    private void startServices() throws Exception {
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
    }

    private void waitForInterruption() throws InterruptedException {
        while (isRunning.get()) {
            Thread.sleep(1000);
            if (!regionWatcher.isRunning() || masterServer == null) {
                System.err.println("关键组件已停止，准备退出主节点");
                isRunning.set(false);
            }
        }
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