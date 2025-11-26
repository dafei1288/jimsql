package com.dafei1288.jimsql.server.protocol;

public enum JimMessageType {
    // 连接管理
    CONNECT_REQUEST(1),
    CONNECT_RESPONSE(2),
    
    // 查询相关
    EXECUTE_QUERY(10),
    QUERY_RESULT(11),
    PREPARE_STATEMENT(12),
    PARAMETER_METADATA(13),
    
    // 更新相关
    EXECUTE_UPDATE(20),
    UPDATE_RESULT(21),
    
    // 事务相关
    BEGIN_TRANSACTION(30),
    COMMIT(31),
    ROLLBACK(32),
    
    // 元数据相关
    GET_TABLES(40),
    GET_COLUMNS(41),
    METADATA_RESULT(42),
    
    // 错误处理
    ERROR(90);
    
    private final int value;
    
    JimMessageType(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return value;
    }
    
    public static JimMessageType valueOf(int value) {
        for (JimMessageType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown message type value: " + value);
    }
} 