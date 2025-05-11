package com.mds.master;

import com.mds.master.self.MasterElection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.UUID;

public class MasterMain {
    public static void main(String[] args) throws Exception {
        String zkAddress = "localhost:2181";
        String masterID = UUID.randomUUID().toString();

        CuratorFramework client = CuratorFrameworkFactory.newClient(
                zkAddress, new ExponentialBackoffRetry(1000, 3)
        );
        client.start();

        MasterElection election = new MasterElection(client, masterID);
        election.start();
//        new MasterElection(client, masterID).start();
        new RegionWatcher(client).startWatching();
        new MasterServer().start(9000);  // 开一个端口接收 Client 请求
    }
}
