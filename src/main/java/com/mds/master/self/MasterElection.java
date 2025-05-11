package com.mds.master.self;

import com.mds.master.MasterDispatcher;
import com.mds.master.RegionWatcher;
import org.apache.curator.framework.CuratorFramework;

import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.util.concurrent.CountDownLatch;

public class MasterElection {
    private final CuratorFramework zkClient;
    private final LeaderSelector leaderSelector;
    private final String MASTER_PATH="/master";     //选主路径
    private final String masterID;                  //主id

    public MasterElection(CuratorFramework zkClient, String masterID)
    {
        this.zkClient = zkClient;
        this.masterID=masterID;

        this.leaderSelector=new LeaderSelector(
                zkClient,
                MASTER_PATH,
                new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception {
                        System.out.println("主节点选举成功！当前主节点 masterID: " + masterID);

                        try {
                            // 启动主节点核心服务
                            MetaManager metaManager = new MetaManager();
                            RegionWatcher regionWatcher = new RegionWatcher(client);
                            regionWatcher.startWatching();
                            MasterDispatcher dispatcher = new MasterDispatcher(metaManager, regionWatcher);

                            // 启动调度服务（阻塞运行）
                            CountDownLatch latch = new CountDownLatch(1);
                            dispatcher.start(); // 启动核心服务
                            latch.await();      // 保持主节点不退出


                        } catch (Exception e) {
                            System.err.println("主节点运行异常：" + masterID);
                            e.printStackTrace();
                        }

                        System.out.println("主节点控制权丢失！ 此主节点为 masterID:"+masterID);

                    }
                }
        );
        this.leaderSelector.autoRequeue();  //掉线则重新参选
    }

    public void start()
    {
        System.out.println("开始选举！");
        this.leaderSelector.start();
    }

    public void close()
    {
        System.out.println("结束选举！");
        this.leaderSelector.close();
    }

}
