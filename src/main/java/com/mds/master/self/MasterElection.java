package com.mds.master.self;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.MasterInfo;
import com.mds.master.MasterDispatcher;
import com.mds.master.RegionWatcher;
import com.mds.master.ZKSyncManager;
import com.mds.master.MasterServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.zookeeper.CreateMode;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Date;

public class MasterElection {
    private static final String MASTER_BASE_PATH = "/master";          
    private static final String ACTIVE_PATH = MASTER_BASE_PATH + "/active";    
    private static final String STANDBY_PATH = MASTER_BASE_PATH + "/standby";  
    private static final int MASTER_PORT = 8000;      

    private final CuratorFramework zkClient;
    private final LeaderSelector leaderSelector;
    private final String masterID;
    private final AtomicBoolean isRunning;
    private final ObjectMapper objectMapper;
    private CountDownLatch masterLatch;
    private volatile MasterServer masterServer;
    private volatile RegionWatcher regionWatcher;
    private volatile MasterDispatcher dispatcher;
    private volatile MetaManager metaManager;
    private volatile ZKSyncManager zkSyncManager;
    private volatile MasterInfo masterInfo;

    public MasterElection(CuratorFramework zkClient, String masterID) {
        this.zkClient = zkClient;
        this.masterID = masterID;
        this.isRunning = new AtomicBoolean(true);
        this.objectMapper = new ObjectMapper();

        try {
            // 初始化MasterInfo
            this.masterInfo = new MasterInfo(
                masterID,
                InetAddress.getLocalHost().getHostAddress(),
                MASTER_PORT,
                "standby",
                System.currentTimeMillis()
            );

            // 初始化ZK路径
            initializePaths();
            
            // 初始化LeaderSelector
            this.leaderSelector = new LeaderSelector(
                zkClient,
                MASTER_BASE_PATH,
                new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception {
                        System.out.println("主节点选举成功！当前主节点 masterID: " + masterID);
                        masterLatch = new CountDownLatch(1);

                        try {
                            // 升级为活跃节点
                            promoteToActive();
                            // 初始化组件
                            initializeComponents(client);
                            // 启动服务
                            startServices();
                            
                            // 保持运行直到被中断
                            while (isRunning.get() && !Thread.currentThread().isInterrupted()) {
                                Thread.sleep(1000);
                                // 检查关键组件状态
                                if (!checkComponentsHealth()) {
                                    break;
                                }
                            }
                        } finally {
                            cleanup();
                        }
                    }
                }
            );
            
            this.leaderSelector.autoRequeue(); // 自动重新参与选举
            
            // 注册为备选节点
            registerAsStandby();
            
        } catch (Exception e) {
            throw new RuntimeException("初始化MasterElection失败", e);
        }
    }

    private void initializePaths() throws Exception {
        createIfNotExists(MASTER_BASE_PATH);
        createIfNotExists(ACTIVE_PATH);
        createIfNotExists(STANDBY_PATH);
    }

    private void createIfNotExists(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);
            System.out.println("创建ZK路径: " + path);
        }
    }

    private void initializeComponents(CuratorFramework client) throws Exception {
        regionWatcher = new RegionWatcher(client);
        zkSyncManager = new ZKSyncManager(client.getZookeeperClient().getZooKeeper());
        metaManager = new MetaManager(regionWatcher);
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

    private boolean checkComponentsHealth() {
        return regionWatcher.isRunning() && masterServer != null && dispatcher != null;
    }

    private void registerAsStandby() {
        try {
            String standbyPath = STANDBY_PATH + "/" + masterID;
            masterInfo.setStatus("standby");
            byte[] data = objectMapper.writeValueAsBytes(masterInfo);
            
            zkClient.create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(standbyPath, data);
            System.out.println("注册为备选节点: " + standbyPath);
            // 节点信息打印
            System.out.println("注册为备选节点成功！节点信息：");
            System.out.println("├── 路径: " + standbyPath);
            System.out.println("├── ID: " + masterInfo.getMasterID());
            System.out.println("├── 地址: " + masterInfo.getHost());
            System.out.println("├── 端口: " + masterInfo.getPort());
            System.out.println("├── 状态: " + masterInfo.getStatus());
            System.out.println("└── 创建时间: " + new Date(masterInfo.getCreateTime()));

        } catch (Exception e) {
            throw new RuntimeException("注册备选节点失败", e);
        }
    }

    private void promoteToActive() throws Exception {
        try {
            // 删除备选节点
            String standbyPath = STANDBY_PATH + "/" + masterID;
            if (zkClient.checkExists().forPath(standbyPath) != null) {
                zkClient.delete().forPath(standbyPath);
            }
            
            // 创建活跃节点
            String activePath = ACTIVE_PATH + "/" + masterID;
            masterInfo.setStatus("active");
            masterInfo.setCreateTime(System.currentTimeMillis());
            byte[] data = objectMapper.writeValueAsBytes(masterInfo);
            
            zkClient.create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(activePath, data);
            System.out.println("升级为活跃主节点: " + activePath);
            // 添加节点信息打印
            System.out.println("升级为活跃主节点成功！节点信息：");
            System.out.println("├── 路径: " + activePath);
            System.out.println("├── ID: " + masterInfo.getMasterID());
            System.out.println("├── 地址: " + masterInfo.getHost());
            System.out.println("├── 端口: " + masterInfo.getPort());
            System.out.println("├── 状态: " + masterInfo.getStatus());
            System.out.println("└── 创建时间: " + new Date(masterInfo.getCreateTime()));
        
        } catch (Exception e) {
            System.err.println("升级为活跃节点失败: " + e.getMessage());
            throw e;
        }
    }

    private void cleanup() {
        System.out.println("开始清理资源...");
        try {
            if (dispatcher != null) {
                dispatcher.stop();
            }
            if (masterServer != null) {
                masterServer.stop();
            }
            if (regionWatcher != null) {
                regionWatcher.stop();
            }
            
            // 降级为备选节点
            demoteToStandby();
            
        } catch (Exception e) {
            System.err.println("清理资源时发生错误: " + e.getMessage());
        } finally {
            if (masterLatch != null) {
                masterLatch.countDown();
            }
            System.out.println("资源清理完成");
        }
    }

    private void demoteToStandby() {
        try {
            String activePath = ACTIVE_PATH + "/" + masterID;
            if (zkClient.checkExists().forPath(activePath) != null) {
                zkClient.delete().forPath(activePath);
            }
            masterInfo.setStatus("standby");
            masterInfo.setCreateTime(System.currentTimeMillis());
            registerAsStandby();
        } catch (Exception e) {
            System.err.println("节点状态转换失败: " + e.getMessage());
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
                masterLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        this.leaderSelector.close();
        System.out.println("主节点已关闭！masterID: " + masterID);
    }

    public MasterInfo getCurrentMasterInfo() {
        return masterInfo;
    }

    public MasterInfo getActiveMasterInfo() throws Exception {
        try {
            List<String> activeNodes = zkClient.getChildren().forPath(ACTIVE_PATH);
            if (activeNodes.isEmpty()) {
                return null;
            }
            
            String activePath = ACTIVE_PATH + "/" + activeNodes.get(0);
            byte[] data = zkClient.getData().forPath(activePath);
            return objectMapper.readValue(data, MasterInfo.class);
        } catch (Exception e) {
            System.err.println("获取活跃主节点信息失败: " + e.getMessage());
            throw e;
        }
    }
}