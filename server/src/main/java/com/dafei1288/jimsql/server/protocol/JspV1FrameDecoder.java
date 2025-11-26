package com.dafei1288.jimsql.server.protocol;

import com.dafei1288.jimsql.common.protocol.FrameHeader;
import com.dafei1288.jimsql.common.protocol.ProtocolFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.nio.ByteBuffer;
import java.util.List;

public class JspV1FrameDecoder extends ByteToMessageDecoder {
  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (in.readableBytes() < FrameHeader.SIZE) {
      return;
    }
    in.markReaderIndex();
    byte[] headerBytes = new byte[FrameHeader.SIZE];
    in.readBytes(headerBytes);
    FrameHeader header = FrameHeader.readFrom(ByteBuffer.wrap(headerBytes));
    if (in.readableBytes() < header.payloadLen) {
      in.resetReaderIndex();
      return;
    }
    byte[] payload = new byte[header.payloadLen];
    in.readBytes(payload);
    out.add(new ProtocolFrame(header, payload));
  }
}

