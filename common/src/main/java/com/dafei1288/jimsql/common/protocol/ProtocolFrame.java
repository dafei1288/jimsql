package com.dafei1288.jimsql.common.protocol;

public class ProtocolFrame {
  public FrameHeader header;
  public byte[] payload;

  public ProtocolFrame(FrameHeader header, byte[] payload) {
    this.header = header;
    this.payload = payload;
  }
}

