package com.dafei1288.jimsql.jdbc.protocol;

import com.dafei1288.jimsql.common.protocol.FrameHeader;
import com.dafei1288.jimsql.common.protocol.MessageType;
import com.dafei1288.jimsql.common.protocol.ProtocolFrame;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class JspV1Wire {
  private final Socket socket;
  private final InputStream in;
  private final OutputStream out;
  private boolean helloDone = false;
  private long reqSeq = 1L;
  private final String wirelog;

  public JspV1Wire(Socket socket, String wirelog) throws IOException {
    this.socket = socket;
    this.in = socket.getInputStream();
    this.out = socket.getOutputStream();
    this.wirelog = wirelog;
  }

  public void ensureHello(String db, String user) throws IOException {
    if (helloDone) return;
    String hello = String.format("{\"client\":\"jdbc\",\"db\":\"%s\",\"user\":\"%s\",\"cap\":{\"compress\":[\"none\",\"lz4\"],\"row\":\"row_bin_v1\",\"cursor\":true,\"tls\":\"preferred\"}}", db, user==null?"":user);
    long rid = nextReqId();
    writeFrame(MessageType.HELLO, rid, hello.getBytes(StandardCharsets.UTF_8));
    ProtocolFrame ack = readFrame();
    if ((ack.header.type & 0xFF) != MessageType.HELLO_ACK.code) {
      throw new IOException("Expected HELLO_ACK, got type=" + (ack.header.type & 0xFF));
    }
    helloDone = true;
  }

  public ProtocolFrame query(String sql, boolean openCursor, int fetchSize, int timeoutMs) throws IOException {
    String q = String.format("{\"sql\":\"%s\",\"openCursor\":%s,\"fetchSize\":%d,\"timeoutMs\":%d}",
        escapeJson(sql), openCursor?"true":"false", fetchSize, timeoutMs);
    long rid = nextReqId();
    writeFrame(MessageType.QUERY, rid, q.getBytes(StandardCharsets.UTF_8));
    return null; // caller will read frames as needed
  }

  public ProtocolFrame readFrame() throws IOException {
    byte[] headerBytes = in.readNBytes(FrameHeader.SIZE);
    if (headerBytes.length < FrameHeader.SIZE) throw new IOException("EOF reading header");
    FrameHeader h = FrameHeader.readFrom(ByteBuffer.wrap(headerBytes));
    byte[] payload = in.readNBytes(h.payloadLen);
    if (payload.length < h.payloadLen) throw new IOException("EOF reading payload");
    ProtocolFrame f = new ProtocolFrame(h, payload);
    if (wirelog != null && !wirelog.isEmpty()) logFrame("S->C", f);
    return f;
  }

  public void writeFrame(MessageType t, long requestId, byte[] payload) throws IOException {
    if (payload == null) payload = new byte[0];
    FrameHeader h = new FrameHeader((byte)1, (byte)t.code, (short)0, requestId, payload.length, 0);
    ByteBuffer buf = ByteBuffer.allocate(FrameHeader.SIZE);
    FrameHeader.writeTo(buf, h);
    out.write(buf.array());
    if (payload.length > 0) out.write(payload);
    out.flush();
    if (wirelog != null && !wirelog.isEmpty()) logFrame("C->S", new ProtocolFrame(h, payload));
  }

  public void fetch(String cursorId, int fetchSize) throws IOException {
    String body = String.format("{\"cursorId\":\"%s\",\"fetchSize\":%d}", cursorId, fetchSize);
    writeFrame(MessageType.CURSOR_FETCH, nextReqId(), body.getBytes(StandardCharsets.UTF_8));
  }

  public void closeCursor(String cursorId) throws IOException {
    String body = String.format("{\"cursorId\":\"%s\"}", cursorId);
    writeFrame(MessageType.CURSOR_CLOSE, nextReqId(), body.getBytes(StandardCharsets.UTF_8));
  }

  private long nextReqId(){
    return reqSeq++;
  }

  private static String escapeJson(String s){
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private void logFrame(String dir, ProtocolFrame f) {
    int type = f.header.type & 0xFF;
    String tname = MessageType.fromCode(type).name();
    if ("json".equalsIgnoreCase(wirelog)) {
      if (type == MessageType.RESULTSET_BATCH.code) {
        System.out.printf("[wirelog] %s %s req=%d flags=%d len=%d%n", dir, tname, f.header.requestId, (int)f.header.flags, f.header.payloadLen);
      } else {
        String body = new String(f.payload, StandardCharsets.UTF_8);
        System.out.printf("[wirelog] %s %s req=%d flags=%d len=%d payload=%s%n", dir, tname, f.header.requestId, (int)f.header.flags, f.header.payloadLen, body);
      }
    } else if ("hex".equalsIgnoreCase(wirelog)) {
      int show = Math.min(64, f.payload.length);
      StringBuilder hex = new StringBuilder();
      for (int i=0;i<show;i++) hex.append(String.format("%02x", f.payload[i]));
      if (f.payload.length > show) hex.append("...");
      System.out.printf("[wirelog] %s %s req=%d flags=%d len=%d hex=%s%n", dir, tname, f.header.requestId, (int)f.header.flags, f.header.payloadLen, hex);
    }
  }
}
