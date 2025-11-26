package com.dafei1288.jimsql.common.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FrameHeader {
  public static final byte[] MAGIC = new byte[] {'J','S','Q','L'};
  public static final int SIZE = 24; // magic(4)+ver(1)+type(1)+flags(2)+reqId(8)+len(4)+resv(4)

  public byte version;
  public byte type;
  public short flags;
  public long requestId;
  public int payloadLen;
  public int reserved;

  public FrameHeader() {}

  public FrameHeader(byte version, byte type, short flags, long requestId, int payloadLen, int reserved) {
    this.version = version;
    this.type = type;
    this.flags = flags;
    this.requestId = requestId;
    this.payloadLen = payloadLen;
    this.reserved = reserved;
  }

  public static void writeTo(ByteBuffer buf, FrameHeader h) {
    buf.put(MAGIC);
    buf.put(h.version);
    buf.put(h.type);
    buf.putShort(h.flags);
    buf.putLong(h.requestId);
    buf.putInt(h.payloadLen);
    buf.putInt(h.reserved);
  }

  public static FrameHeader readFrom(ByteBuffer buf) {
    byte[] magic = new byte[4];
    buf.get(magic);
    if (!(magic[0]=='J' && magic[1]=='S' && magic[2]=='Q' && magic[3]=='L')) {
      String m = new String(magic, StandardCharsets.US_ASCII);
      throw new IllegalArgumentException("Bad magic: " + m);
    }
    FrameHeader h = new FrameHeader();
    h.version = buf.get();
    h.type = buf.get();
    h.flags = buf.getShort();
    h.requestId = buf.getLong();
    h.payloadLen = buf.getInt();
    h.reserved = buf.getInt();
    return h;
  }
}

