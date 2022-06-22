package com.dafei1288.jimsql.server;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Jimserver {

  private static String HOST = "0.0.0.0";
  private static int PORT = 8821;
  private static String DATA_DIR = "./data" ;


  public static String getDataDir(){
    return DATA_DIR;
  }
  public static int getPort(){
    return PORT;
  }
  public static String getHost(){
    return HOST;
  }

  private static EventLoopGroup bossGroup;
  private static EventLoopGroup workerGroup;

  private static ChannelFuture channelFuture;

  public static void createServer(String host,int port,String datadir){
    DATA_DIR = datadir;
    PORT = port;
    HOST = host;

    //循环组接收连接，不进行处理,转交给下面的线程组
    bossGroup = new NioEventLoopGroup();
    //循环组处理连接，获取参数，进行工作处理
    workerGroup = new NioEventLoopGroup();
    try {
      //服务端进行启动类
      ServerBootstrap serverBootstrap = new ServerBootstrap();
      //使用NIO模式，初始化器等等
      serverBootstrap.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new JimServerInitializer());
      //绑定端口
      channelFuture = serverBootstrap.bind(host,port).sync();
          //serverBootstrap.bind(host,port).sync();
      System.out.println(String.format("jimsql server is running on %s:%s , with data dir : %s ",host,port,datadir));
      channelFuture.channel().closeFuture().sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  void shutdown(){
    System.out.println("Stopping server");
    try{
      bossGroup.shutdownGracefully().sync();
      workerGroup.shutdownGracefully().sync();
      channelFuture.channel().close();
      System.out.println("Server stopped");
    }catch (InterruptedException e){
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Integer port = 8821;
    String host = "0.0.0.0";
    String datadir = "data";
    if(args.length == 1){
      port = Integer.parseInt(args[0]);
    }else if(args.length == 2){
      port = Integer.parseInt(args[0]);
      host = args[1];
    }else if(args.length ==3){
      port = Integer.parseInt(args[0]);
      host = args[1];
      datadir = args[2];
    }
    createServer(host,port,datadir);
  }
}
