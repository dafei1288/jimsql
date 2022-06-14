import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class JimClient {

  private JimClientInitializer jimClientInitializer;
  private JimClientHandler jimClientHandler;


  public static void main(String[] args) {

    JimClientInitializer jimClientInitializer = new JimClientInitializer();
    JimClientHandler jimClientHandler = new JimClientHandler();

    jimClientInitializer.setJimClientHandler(jimClientHandler);

    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    try {
      Bootstrap bootstrap = new Bootstrap();
      bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class).handler(jimClientInitializer);
      //绑定服务器
      ChannelFuture channelFuture = bootstrap.connect("127.0.0.1", 9008).sync();
//      channelFuture.channel().closeFuture().sync();

      System.out.println("212121221");
      channelFuture.channel().writeAndFlush("this is 1");
      channelFuture.channel().writeAndFlush("this is 2 ");
      Thread.sleep(1000);
      channelFuture.channel().writeAndFlush("this is 3");
//      jimClientHandler.writeMessage("this is 1");
//      jimClientHandler.writeMessage("this is 2");

    } catch (Exception e) {
      e.printStackTrace();
      eventLoopGroup.shutdownGracefully();
    }
  }

}
