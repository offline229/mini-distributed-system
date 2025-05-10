package com.mds.region.constant;

public class JsonFormat {
    // 操作类型
    public static final String OPERATION_QUERY = "QUERY";
    public static final String OPERATION_INSERT = "INSERT";
    public static final String OPERATION_UPDATE = "UPDATE";
    public static final String OPERATION_DELETE = "DELETE";

    // JSON 字段名
    public static final String FIELD_OPERATION = "operation";
    public static final String FIELD_SQL = "sql";
    public static final String FIELD_PARAMS = "params";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_DATA = "data";
    public static final String FIELD_MESSAGE = "message";

    // 状态值
    public static final String STATUS_SUCCESS = "success";
    public static final String STATUS_ERROR = "error";

    // 示例JSON
    public static final String REQUEST_EXAMPLE = "{\n" +
            "    \"operation\": \"QUERY\",\n" +
            "    \"sql\": \"SELECT * FROM users WHERE id = ?\",\n" +
            "    \"params\": [\"1\"]\n" +
            "}";

    public static final String RESPONSE_EXAMPLE = "{\n" +
            "    \"status\": \"success\",\n" +
            "    \"data\": [],\n" +
            "    \"message\": \"\"\n" +
            "}";
}