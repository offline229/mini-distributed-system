### 【第一阶段】：实现一个最小可运行的Master模块
#### 1.ZooKeeper 注册与选主（LeaderSelector）
- 在MasterElection里用Curator LeaderSelector进行选举
#### 2.Region 上下线监听（监听 /regions 临时节点）
- RegionWatcher被动监听心跳变化
#### 3.Region 注册信息写入 MySQL（使用你的 MySQLUtil）
- MasterServer
  - 接受请求
  - 分配唯一rid，创建regionInfo
    - RegionInfo：存rid，host，port，load（负载信息、可选），createTime
  - 调用写入两个模块的函数
  - 返回rid给region
- 【先】使用MetaManager写入MySQL——长期管理元数据
  - 建表regions管理info的信息 table=regionInfo
- 【后】使用ZKSyncManager写入zk——短期协调、心跳 
- 
#### 4.Master 自己暴露一个 Socket 服务端口（接收 Client 的 SQL 请求）
- 对 Client 的请求做简单响应（返回：你已连接到 Master）



### 【子模块划分】
MasterElection: 处理选主逻辑。
RegionWatcher: 监听ZooKeeper节点，维护在线Region列表。
ZKSyncManager: 管理ZooKeeper的连接和路径操作。
MetaManager: 管理MySQL中存储的元信息。
MasterServer: 启动服务监听Client请求。
MasterDispatcher: 用于解析和分发SQL请求
#### 1.MasterElection：使用 Curator LeaderSelector 做主节点选举
- ZooKeeper（Curator）保证同一时间只有一个 Master 节点处于“主控状态”，并且在当前 Master 宕机时能自动切换到其他候选 Master 节点。
- 什么是leader selector？
  - Curator 提供的 LeaderSelector 是对 ZooKeeper 临时顺序节点选举机制的封装，用来实现主节点选举和切换，它能： 
    - 自动参选并阻塞其他候选节点 
    - 当前主节点挂掉后，自动触发重新选举 
    - 自动回调一个 takeLeadership() 方法 → 你在这方法里写“我成为主”的逻辑

#### 2.RegionWatcher：监听 ZooKeeper /regions，维护 Region 列表
- 注册 ZK watcher（使用 PathChildrenCache）
- 提供 getOnlineRegions() 接口供调度器等模块调用
- 
#### 3.MetaManager：使用 MySQLUtil 存储 region 和表的元信息
- saveRegionInfo(RegionInfo info)：新增或更新 Region 记录
- getAllRegions()：恢复 Region 列表（如主节点切换）
- 
#### 4.MasterServer：启动 socket 服务，接收 Client 请求并简单回应

#### 5.ZKSyncManager：封装 ZooKeeper 的连接和基本路径管理
- registerRegion(RegionInfo info)： 
  - 创建 /regions/{regionId} 临时节点 
  - 写入 JSON 数据：{ regionId, host, port, status, load } 
- updateRegionStatus(regionId, newStatus)：可选扩展
- 
#### 6.Main：启动类，初始化上面所有模块并启动主循环

## 和region交互流程
- Region 节点启动后注册到 Master； 
- Master 分配并返回 RegionId，将 Region 信息写入 ZooKeeper 和 MySQL； 
- Master 实时监听 Region 状态变化（上下线、状态更新）； 
- Region 节点宕机后 ZooKeeper 自动删除临时节点，Master 监听并处理。
 
 Region                        MasterServer                ZooKeeper              MySQL
  |                               |                            |                     |
  |--连接ZooKeeper（准备）------>|                            |                     |
  |--连接Master并发送host+port-->|                            |                     |
  |                               |--生成regionId------------>|                     |
  |                               |--构造RegionInfo对象--------|                     |
  |                               |--写入MySQL---------------->|--INSERT INTO table--|
  |                               |--写入ZooKeeper------------>|--/regions/rid {json}|
  |                               |<----------regionId--------|                     |
  |--保存regionId，启动服务------>|                            |                     |
