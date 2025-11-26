package com.dafei1288.jimsql.common.protocol;

public enum MessageType {
  HELLO(0x01),
  HELLO_ACK(0x02),
  OK(0x05),
  ERROR(0x06),

  QUERY(0x10),
  RESULTSET_HEADER(0x11),
  RESULTSET_BATCH(0x12),
  RESULTSET_END(0x13),
  UPDATE_COUNT(0x14),

  CURSOR_FETCH(0x19),
  CURSOR_CLOSE(0x1B),

  PING(0x30),
  PONG(0x31),
  CANCEL(0x32),
  SERVER_STATUS(0x34);

  public final int code;

  MessageType(int code) {
    this.code = code;
  }

  public static MessageType fromCode(int code) {
    for (MessageType t : values()) {
      if (t.code == code) return t;
    }
    throw new IllegalArgumentException("Unknown MessageType code: " + code);
  }
}

