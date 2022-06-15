package com.dafei1288.jimsql.server;


import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectDecoderInputStream;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;


public class JimServerInitializer  extends ChannelInitializer<SocketChannel> {

  //连接注册，创建成功，会被调用
  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
//    pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
//    pipeline.addLast(new LengthFieldPrepender(4));
//    pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
    pipeline.addLast(new ObjectEncoder());
//    pipeline.addLast(new ObjectDecoder());
    pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
    pipeline.addLast(new JimServerHandler());
  }
}
