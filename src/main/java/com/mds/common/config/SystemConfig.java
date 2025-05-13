package com.mds.common.config;

public class SystemConfig {
    // ZooKeeper配置
    public static final String ZK_CONNECT_STRING = "localhost:2181";
    public static final int ZK_SESSION_TIMEOUT = 5000;
    public static final String ZK_ROOT_PATH = "/mini-distributed-system";
    public static final String ZK_REGION_PATH = ZK_ROOT_PATH + "/regions";
    public static final String ZK_MASTER_PATH = ZK_ROOT_PATH + "/master";
    public static final String ZK_TABLE_PATH = ZK_ROOT_PATH + "/tables";

    // MySQL配置
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306/mini_distributed_system?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final int MYSQL_POOL_SIZE = 10;

    // 系统配置
    public static final int HEARTBEAT_INTERVAL = 3000; // 心跳间隔（毫秒）
    public static final int HEARTBEAT_TIMEOUT = 10000; // 心跳超时（毫秒）
    public static final int REGION_PORT = 8080; // 区域节点默认端口
    public static final int MASTER_PORT = 8081; // 主节点默认端口

    // 状态常量
    public static final String STATUS_ONLINE = "ONLINE";
    public static final String STATUS_OFFLINE = "OFFLINE";
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_INACTIVE = "INACTIVE";
} 