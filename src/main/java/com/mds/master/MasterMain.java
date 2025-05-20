package com.mds.master;

import com.mds.master.self.MasterElection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.UUID;

public class MasterMain {
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final int ZK_TIMEOUT = 5000;
    private static final int ZK_RETRY_TIMES = 3;
    private static final int ZK_RETRY_INTERVAL = 1000;

    public static void main(String[] args) {
        CuratorFramework client = null;
        MasterElection election = null;

        try {
            // 生成唯一的 masterID
            String masterID = UUID.randomUUID().toString();
            System.out.println("正在启动 Master 节点, ID: " + masterID);

            // 初始化 ZooKeeper 客户端
            client = CuratorFrameworkFactory.builder()
                    .connectString(ZK_ADDRESS)
                    .sessionTimeoutMs(ZK_TIMEOUT)
                    .retryPolicy(new ExponentialBackoffRetry(ZK_RETRY_INTERVAL, ZK_RETRY_TIMES))
                    .build();
            
            client.start();
            System.out.println("ZooKeeper 客户端已启动");

            // 等待连接建立
            if (!client.blockUntilConnected(ZK_TIMEOUT, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                throw new RuntimeException("连接 ZooKeeper 超时");
            }

            // 创建并启动选举程序
            final CuratorFramework finalClient = client;
            election = new MasterElection(finalClient, masterID);
            election.start();   // 启动选举程序
            System.out.println("Master 选举程序已启动");

            // 添加关闭钩子
            final MasterElection finalElection = election;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("正在关闭 Master 节点...");
                if (finalElection != null) {
                    finalElection.close();
                }
                if (finalClient != null) {
                    finalClient.close();
                }
                System.out.println("Master 节点已安全关闭");
            }));

            // 保持程序运行
            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("Master 节点运行异常: " + e.getMessage());
            e.printStackTrace();
            
            // 发生异常时清理资源
            if (election != null) {
                election.close();
            }
            if (client != null) {
                client.close();
            }
            System.exit(1);
        }
    }
}