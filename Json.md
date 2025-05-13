# 消息格式整理

// ZK中存储的Master节点信息
{
    "host": "localhost",
    "port": 9000
}

// Region向Master发送的注册请求
{
    "type": "REGISTER",
    "host": "localhost",
    "port": 8000
}

// Region向Master发送的心跳
{
    "type": "HEARTBEAT",
    "timestamp": 1234567890,
    "status": "ACTIVE"
}

// Client向Region发送的SQL请求
{
    "operation": "SELECT",  // SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE
    "sql": "SELECT * FROM test_table",
    "params": ["param1", "param2"]  // 可选参数
}

// Region向Client返回的响应
{
    "status": "success",   // success, error
    "data": [],           // 查询结果或影响行数
    "operation": "SELECT", // 操作类型
    "tableName": "test_table", // 表名
    "isDataChanged": false, // 是否改变了数据
    "message": "查询成功"  // 操作结果描述
}

// Region在ZK中的状态信息
{
    "status": "ACTIVE",
    "lastOperation": 1234567890,
    "load": 0.75,
    "tables": {
        "table1": {
            "0-100": "region_1",
            "101-200": "region_2"
        },
        "table2": {
            "0-50": "region_1"
        }
    }
}

// 错误响应通用格式
{
    "status": "error",
    "code": "ERROR_CODE",
    "message": "错误描述信息"
}

// DBHandler执行结果
{
    "data": [],           // 执行结果数据
    "operation": "SELECT", // 操作类型
    "tableName": "test_table", // 表名
    "isDataChanged": false, // 是否改变了数据
    "message": "查询成功"  // 附加信息
}

// RegionServer数据变更通知
{
    "operation": "INSERT", // 操作类型
    "tableName": "test_table", // 表名
    "data": 1,            // 影响行数或其他数据
    "message": "影响行数: 1" // 操作结果描述
}
