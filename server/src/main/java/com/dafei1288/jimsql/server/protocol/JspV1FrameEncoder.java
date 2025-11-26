package com.dafei1288.jimsql.server.protocol;

import com.dafei1288.jimsql.common.protocol.FrameHeader;
import com.dafei1288.jimsql.common.protocol.ProtocolFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.nio.ByteBuffer;

public class JspV1FrameEncoder extends MessageToByteEncoder<ProtocolFrame> {
  @Override
  protected void encode(ChannelHandlerContext ctx, ProtocolFrame msg, ByteBuf out) throws Exception {
    ByteBuffer headerBuf = ByteBuffer.allocate(FrameHeader.SIZE);
    FrameHeader.writeTo(headerBuf, msg.header);
    headerBuf.flip();
    out.writeBytes(headerBuf);
    if (msg.payload != null && msg.payload.length > 0) {
      out.writeBytes(msg.payload);
    }
  }
}

