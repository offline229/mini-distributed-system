 # 分布式系统示例项目

这是一个基于ZooKeeper和MySQL的分布式系统示例项目，用于演示分布式系统的基本概念和实现。

## 环境要求

### 操作系统
- Windows 10/11

### 开发环境
- JDK 17+
- Maven 3.8+
- MySQL 8.0.27
- ZooKeeper 3.8.4

### 依赖版本
- ZooKeeper: 3.8.4
- MySQL Connector: 8.0.27
- SLF4J: 1.7.30
- Logback: 1.2.6
- JUnit: 4.13.2

## 项目结构

```
mini-distributed-system/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/mds/
│   │   │       ├── common/     # 公共组件
│   │   │       ├── master/     # 主节点
│   │   │       ├── region/     # 区域节点
│   │   │       └── client/     # 客户端
│   │   └── resources/          # 配置文件
├── test/
│   ├── java/com/mds
│   │   ├── common/test
│   │   │   ├── xxxx.java       # 测试代码
│   └── resources/              # 配置文件
└── pom.xml                     # Maven配置
```

## 配置说明

### ZooKeeper配置
1. 下载并安装ZooKeeper 3.8.4
2. 复制`conf/zoo_sample.cfg`为`conf/zoo.cfg`
3. 修改`zoo.cfg`中的配置：
   ```properties
   dataDir=D:/zookeeper/data
   clientPort=2181
   ```
4. 启动ZooKeeper：
   ```bash
   bin/zkServer.cmd
   ```

### MySQL配置
1. 安装MySQL 8.0.27
2. 创建测试数据库：
   ```sql
   CREATE DATABASE test;
   ```
3. 配置数据库连接：
   - 用户名：root
   - 密码：自行修改
   - 数据库：test
   - 端口：3306

### 日志配置
项目使用Logback进行日志管理，配置文件位于：
- 主配置：`src/main/resources/logback.xml`
- 测试配置：`test/src/main/resources/logback-test.xml`

## 启动说明

### 启动ZooKeeper
1. 进入ZooKeeper安装目录
2. 运行启动脚本：
   ```bash
   bin/zkServer.cmd
   ```
   或直接点击该文件启动

### 运行测试
1. 确保ZooKeeper和MySQL服务已启动
2. 运行所有测试：
   ```bash
   mvn test
   ```
3. 运行特定测试：
   ```bash
   mvn test -Dtest=ZKConnectTest
   mvn test -Dtest=MySQLConnectTest
   ```

## 注意事项

1. 确保所有服务端口未被占用：
   - ZooKeeper: 2181
   - MySQL: 3306

2. 首次运行前需要：
   - 创建ZooKeeper数据目录
   - 创建MySQL数据库
   - 配置正确的数据库连接信息

3. 开发环境变量：
   - JAVA_HOME: JDK 17安装目录
   - MAVEN_HOME: Maven安装目录
   - PATH: 包含%JAVA_HOME%\bin和%MAVEN_HOME%\bin

## 常见问题

1. ZooKeeper连接失败
   - 检查ZooKeeper服务是否启动
   - 确认端口2181未被占用
   - 检查防火墙设置

2. MySQL连接失败
   - 确认MySQL服务已启动
   - 检查用户名密码是否正确
   - 确认数据库已创建

3. 编译错误
   - 确认JDK版本为17+
   - 检查Maven配置
   - 确保所有依赖下载完成