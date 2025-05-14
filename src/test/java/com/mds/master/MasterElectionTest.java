package com.mds.master;

import com.mds.master.self.MasterElection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.common.MasterInfo;
import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 主节点选举机制单元测试
 */
public class MasterElectionTest {

    private CuratorFramework zkClient;
    private MasterElection masterElection;

    @BeforeEach
    public void setUp() throws Exception {
        // Mock ZK客户端
        zkClient = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
        // Mock ZK路径存在
        when(zkClient.checkExists().forPath(anyString())).thenReturn(mock(org.apache.zookeeper.data.Stat.class));
        // 构造MasterElection
        masterElection = new MasterElection(zkClient, "test-master-id");
    }

    @Test
    public void testMasterElectionInit() {
        assertNotNull(masterElection);
        assertEquals("test-master-id", masterElection.getCurrentMasterInfo().getMasterID());
    }

    @Test
    public void testRegisterAsStandbyAndPromoteToActive() throws Exception {
        // 模拟ZK操作
        when(zkClient.create().creatingParentsIfNeeded().withMode(any()).forPath(anyString(), any(byte[].class)))
                .thenReturn("/mds/master/standby/test-master-id");

        // 设置为standby
        masterElection.getCurrentMasterInfo().setStatus("standby");
        masterElection.getCurrentMasterInfo().setCreateTime(System.currentTimeMillis());

        // 通过反射调用promoteToActive（假设是private）
        assertDoesNotThrow(() -> {
            var method = masterElection.getClass().getDeclaredMethod("promoteToActive");
            method.setAccessible(true);
            method.invoke(masterElection);
        });
    }

    @Test
    public void testGetActiveMasterInfo() throws Exception {
        // 模拟活跃节点
        when(zkClient.getChildren().forPath(anyString())).thenReturn(List.of("test-master-id"));
        MasterInfo info = new MasterInfo("test-master-id", "localhost", 8000, "active", System.currentTimeMillis());
        byte[] data = new ObjectMapper().writeValueAsBytes(info);
        when(zkClient.getData().forPath(anyString())).thenReturn(data);

        MasterInfo activeInfo = masterElection.getActiveMasterInfo();
        assertNotNull(activeInfo);
        assertEquals("test-master-id", activeInfo.getMasterID());
        assertEquals("active", activeInfo.getStatus());
    }

    @Test
    public void testMultipleNodesRegisterAndElectLeader() throws Exception {
        // 模拟多个节点注册
        CuratorFramework zkClient2 = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
        CuratorFramework zkClient3 = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);

        when(zkClient2.checkExists().forPath(anyString())).thenReturn(mock(org.apache.zookeeper.data.Stat.class));
        when(zkClient3.checkExists().forPath(anyString())).thenReturn(mock(org.apache.zookeeper.data.Stat.class));

        MasterElection node1 = new MasterElection(zkClient, "node-1");
        MasterElection node2 = new MasterElection(zkClient2, "node-2");
        MasterElection node3 = new MasterElection(zkClient3, "node-3");

        // 模拟ZK活跃节点列表
        List<String> activeNodes = new ArrayList<>(List.of("node-2"));
        when(zkClient.getChildren().forPath(anyString())).thenReturn(activeNodes);
        when(zkClient2.getChildren().forPath(anyString())).thenReturn(activeNodes);
        when(zkClient3.getChildren().forPath(anyString())).thenReturn(activeNodes);

        MasterInfo info = new MasterInfo("node-2", "localhost", 8000, "active", System.currentTimeMillis());
        byte[] data = new ObjectMapper().writeValueAsBytes(info);
        when(zkClient.getData().forPath(anyString())).thenReturn(data);
        when(zkClient2.getData().forPath(anyString())).thenReturn(data);
        when(zkClient3.getData().forPath(anyString())).thenReturn(data);

        MasterInfo leader1 = node1.getActiveMasterInfo();
        MasterInfo leader2 = node2.getActiveMasterInfo();
        MasterInfo leader3 = node3.getActiveMasterInfo();

        assertNotNull(leader1);
        assertNotNull(leader2);
        assertNotNull(leader3);
        assertEquals("node-2", leader1.getMasterID());
        assertEquals("node-2", leader2.getMasterID());
        assertEquals("node-2", leader3.getMasterID());
    }

    @Test
public void testLeaderFailoverAndReelection() throws Exception {
    // 模拟两个节点
    CuratorFramework zkClientA = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
    CuratorFramework zkClientB = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);

    when(zkClientA.checkExists().forPath(anyString())).thenReturn(mock(org.apache.zookeeper.data.Stat.class));
    when(zkClientB.checkExists().forPath(anyString())).thenReturn(mock(org.apache.zookeeper.data.Stat.class));

    MasterElection nodeA = new MasterElection(zkClientA, "node-A");
    MasterElection nodeB = new MasterElection(zkClientB, "node-B");

    // 初始活跃节点为node-A
    AtomicReference<List<String>> activeNodes = new AtomicReference<>(List.of("node-A"));
    when(zkClientA.getChildren().forPath(anyString())).thenAnswer((Answer<List<String>>) invocation -> activeNodes.get());
    when(zkClientB.getChildren().forPath(anyString())).thenAnswer((Answer<List<String>>) invocation -> activeNodes.get());

    MasterInfo infoA = new MasterInfo("node-A", "localhost", 8000, "active", System.currentTimeMillis());
    MasterInfo infoB = new MasterInfo("node-B", "localhost", 8000, "active", System.currentTimeMillis());
    ObjectMapper mapper = new ObjectMapper();

    when(zkClientA.getData().forPath(anyString())).thenReturn(mapper.writeValueAsBytes(infoA));
    when(zkClientB.getData().forPath(anyString())).thenReturn(mapper.writeValueAsBytes(infoA));

    // node-A为主节点
    MasterInfo leader = nodeA.getActiveMasterInfo();
    System.out.println("初始主节点: " + leader.getMasterID());
    assertNotNull(leader);
    assertEquals("node-A", leader.getMasterID());

    // 模拟node-A失效，node-B成为主节点
    activeNodes.set(List.of("node-B"));
    when(zkClientA.getData().forPath(anyString())).thenReturn(mapper.writeValueAsBytes(infoB));
    when(zkClientB.getData().forPath(anyString())).thenReturn(mapper.writeValueAsBytes(infoB));

    MasterInfo newLeaderA = nodeA.getActiveMasterInfo();
    MasterInfo newLeaderB = nodeB.getActiveMasterInfo();

    System.out.println("主节点失效后，新的主节点: " + newLeaderA.getMasterID());
    assertNotNull(newLeaderA);
    assertNotNull(newLeaderB);
    assertEquals("node-B", newLeaderA.getMasterID());
    assertEquals("node-B", newLeaderB.getMasterID());
}

    @Test
    public void testNoActiveMaster() throws Exception {
        // 模拟没有活跃主节点
        when(zkClient.getChildren().forPath(anyString())).thenReturn(Collections.emptyList());
        MasterInfo info = masterElection.getActiveMasterInfo();
        assertNull(info);
    }
}