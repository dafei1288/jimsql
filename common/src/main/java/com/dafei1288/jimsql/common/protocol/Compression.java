package com.dafei1288.jimsql.common.protocol;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public final class Compression {
  private static final LZ4Factory F = LZ4Factory.fastestInstance();
  private static final LZ4Compressor COMP = F.fastCompressor();
  private static final LZ4FastDecompressor DECOMP = F.fastDecompressor();

  private Compression() {}

  public static byte[] lz4(byte[] data) {
    int max = COMP.maxCompressedLength(data.length);
    byte[] out = new byte[max + 4];
    out[0] = (byte)((data.length >>> 24) & 0xFF);
    out[1] = (byte)((data.length >>> 16) & 0xFF);
    out[2] = (byte)((data.length >>> 8) & 0xFF);
    out[3] = (byte)(data.length & 0xFF);
    int len = COMP.compress(data, 0, data.length, out, 4, max);
    byte[] res = new byte[len + 4];
    System.arraycopy(out, 0, res, 0, res.length);
    return res;
  }

  public static byte[] unlz4(byte[] data) {
    int origLen = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
    byte[] res = new byte[origLen];
    DECOMP.decompress(data, 4, res, 0, origLen);
    return res;
  }
}

