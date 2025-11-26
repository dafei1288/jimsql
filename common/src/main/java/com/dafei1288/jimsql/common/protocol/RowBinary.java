package com.dafei1288.jimsql.common.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class RowBinary {
  private RowBinary() {}

  // Skeleton helpers: encode/decode empty batches for now.
  public static void writeEmptyBatch(OutputStream out) throws IOException {
    VarInt.writeUVarLong(out, 0L); // rowCount=0
  }

  public static long readRowCount(InputStream in) throws IOException {
    return VarInt.readUVarLong(in);
  }

  // Simple string-only batch: null bitmap set to 0s (no nulls), values are UTF-8 with uvarint length
  public static void writeStringBatch(OutputStream out, java.util.List<String[]> rows, int colCount) throws IOException {
    VarInt.writeUVarLong(out, rows.size());
    int nullBitmapSize = (colCount + 7) / 8;
    byte[] zeros = new byte[nullBitmapSize];
    for (String[] row : rows) {
      out.write(zeros);
      for (int c = 0; c < colCount; c++) {
        String v = (c < row.length) ? row[c] : null;
        if (v == null) v = "";
        byte[] b = v.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        VarInt.writeUVarLong(out, b.length);
        out.write(b);
      }
    }
  }

  public static java.util.List<String[]> readStringBatch(InputStream in, int colCount) throws IOException {
    long rc = readRowCount(in);
    int rowCount = (int) rc;
    int nullBitmapSize = (colCount + 7) / 8;
    java.util.List<String[]> rows = new java.util.ArrayList<>(rowCount);
    for (int r = 0; r < rowCount; r++) {
      // skip null bitmap (no nulls encoded in this simple writer)
      in.readNBytes(nullBitmapSize);
      String[] row = new String[colCount];
      for (int c = 0; c < colCount; c++) {
        long len = VarInt.readUVarLong(in);
        byte[] b = in.readNBytes((int) len);
        row[c] = new String(b, java.nio.charset.StandardCharsets.UTF_8);
      }
      rows.add(row);
    }
    return rows;
  }

  // Typed batch: supports VARCHAR(12), INTEGER(4), BIGINT(-5), DOUBLE(8)
  public static void writeTypedBatch(OutputStream out, java.util.List<String[]> rows, int colCount, int[] types) throws IOException {
    VarInt.writeUVarLong(out, rows.size());
    int nullBitmapSize = (colCount + 7) / 8;
    for (String[] row : rows) {
      // build null bitmap
      byte[] nb = new byte[nullBitmapSize];
      for (int c = 0; c < colCount; c++) {
        boolean isNull = (row[c] == null);
        if (isNull) {
          int byteIdx = c >> 3;
          int bit = c & 7;
          nb[byteIdx] |= (1 << bit);
        }
      }
      out.write(nb);
      for (int c = 0; c < colCount; c++) {
        String v = row[c];
        if (v == null) continue; // null indicated in bitmap
        int t = (types != null && c < types.length) ? types[c] : 12; // default VARCHAR
        switch (t) {
          case 4: { // INTEGER
            int ival;
            try { ival = Integer.parseInt(v); } catch (Exception e) { ival = 0; }
            out.write((ival >>> 24) & 0xFF);
            out.write((ival >>> 16) & 0xFF);
            out.write((ival >>> 8) & 0xFF);
            out.write(ival & 0xFF);
            break; }
          case -5: { // BIGINT
            long l;
            try { l = Long.parseLong(v); } catch (Exception e) { l = 0L; }
            for (int i=7;i>=0;i--) out.write((int)((l >>> (i*8)) & 0xFF));
            break; }
          case 8: { // DOUBLE
            double d;
            try { d = Double.parseDouble(v); } catch (Exception e) { d = 0.0; }
            long bits = Double.doubleToRawLongBits(d);
            for (int i=7;i>=0;i--) out.write((int)((bits >>> (i*8)) & 0xFF));
            break; }
          default: { // VARCHAR and others as string
            byte[] b = v.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            VarInt.writeUVarLong(out, b.length);
            out.write(b);
          }
        }
      }
    }
  }

  public static java.util.List<String[]> readTypedBatchToStrings(InputStream in, int colCount, int[] types) throws IOException {
    long rc = readRowCount(in);
    int rowCount = (int) rc;
    int nullBitmapSize = (colCount + 7) / 8;
    java.util.List<String[]> rows = new java.util.ArrayList<>(rowCount);
    for (int r = 0; r < rowCount; r++) {
      byte[] nb = in.readNBytes(nullBitmapSize);
      String[] row = new String[colCount];
      for (int c = 0; c < colCount; c++) {
        boolean isNull = ((nb[c >> 3] >> (c & 7)) & 1) == 1;
        if (isNull) { row[c] = null; continue; }
        int t = (types != null && c < types.length) ? types[c] : 12;
        switch (t) {
          case 4: { // INTEGER
            byte[] b = in.readNBytes(4);
            int v = ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
            row[c] = Integer.toString(v);
            break; }
          case -5: { // BIGINT
            byte[] b = in.readNBytes(8);
            long v = 0L;
            for (int i=0;i<8;i++) v = (v << 8) | (b[i] & 0xFF);
            row[c] = Long.toString(v);
            break; }
          case 8: { // DOUBLE
            byte[] b = in.readNBytes(8);
            long bits = 0L;
            for (int i=0;i<8;i++) bits = (bits << 8) | (b[i] & 0xFF);
            double d = Double.longBitsToDouble(bits);
            row[c] = Double.toString(d);
            break; }
          default: {
            long len = VarInt.readUVarLong(in);
            byte[] b = in.readNBytes((int) len);
            row[c] = new String(b, java.nio.charset.StandardCharsets.UTF_8);
          }
        }
      }
      rows.add(row);
    }
    return rows;
  }
}
