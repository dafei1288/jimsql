package com.dafei1288.jimsql.server;

import com.dafei1288.jimsql.server.protocol.JspV1FrameDecoder;
import com.dafei1288.jimsql.server.protocol.JspV1FrameEncoder;
import com.dafei1288.jimsql.server.protocol.JspV1ServerHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class JimServerV1Initializer extends ChannelInitializer<SocketChannel> {
  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline p = ch.pipeline();
    String wirelog = System.getProperty("jimsql.wirelog", System.getenv("JIMSQL_WIRELOG"));
    if (wirelog != null && wirelog.equalsIgnoreCase("hex")) {
      p.addLast("wirelog", new LoggingHandler(LogLevel.DEBUG));
    }
    p.addLast(new JspV1FrameDecoder());
    p.addLast(new JspV1FrameEncoder());
    p.addLast(new JspV1ServerHandler());
  }
}
