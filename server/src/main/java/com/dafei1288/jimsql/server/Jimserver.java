package com.dafei1288.jimsql.server;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Jimserver {

  public static String HOST = "0.0.0.0";
  public static int PORT = 8821;

  public static void createServer(String host,int port){
    //循环组接收连接，不进行处理,转交给下面的线程组
    EventLoopGroup bossGroup = new NioEventLoopGroup();
    //循环组处理连接，获取参数，进行工作处理
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      //服务端进行启动类
      ServerBootstrap serverBootstrap = new ServerBootstrap();
      //使用NIO模式，初始化器等等
      serverBootstrap.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new JimServerInitializer());

      //绑定端口
      ChannelFuture channelFuture = serverBootstrap.bind(host,port).sync();
      System.out.println(String.format("jimsql server is running on %s:%s",host,port));
      channelFuture.channel().closeFuture().sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  public static void createServer(int port){
    createServer(HOST,port);
  }
  public static void createServer(){
    createServer(HOST,PORT);
  }


  public static void main(String[] args) {
    createServer();
  }
}
