package com.dafei1288.jimsql.common.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class VarInt {
  private VarInt() {}

  public static void writeUVarLong(OutputStream out, long value) throws IOException {
    while ((value & ~0x7FL) != 0) {
      out.write((int)((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    out.write((int)value);
  }

  public static long readUVarLong(InputStream in) throws IOException {
    long value = 0;
    int shift = 0;
    while (shift < 64) {
      int b = in.read();
      if (b == -1) throw new IOException("EOF reading varint");
      value |= (long)(b & 0x7F) << shift;
      if ((b & 0x80) == 0) return value;
      shift += 7;
    }
    throw new IOException("Varint too long");
  }
}

