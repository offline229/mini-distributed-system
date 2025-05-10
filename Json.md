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
    "operation": "QUERY",  // QUERY, INSERT, UPDATE, DELETE
    "sql": "SELECT * FROM test_table",
    "params": ["param1", "param2"]  // 可选参数
}

// Region向Client返回的响应
{
    "status": "success",   // success, error
    "data": [],           // 查询结果或影响行数
    "message": ""         // 错误信息（如果有）
}

// Region在ZK中的状态信息
{
    "status": "ACTIVE",
    "lastOperation": 1234567890,
    "load": 0.75
}

// 错误响应通用格式
{
    "status": "error",
    "code": "ERROR_CODE",
    "message": "错误描述信息"
}