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

  private String host = "127.0.0.1";
  private int port = 9008;

  private JimClientInitializer jimClientInitializer;
  private JimClientHandler jimClientHandler;


  public ChannelFuture  connect() {

    JimClientInitializer jimClientInitializer = new JimClientInitializer();
    JimClientHandler jimClientHandler = new JimClientHandler();

    jimClientInitializer.setJimClientHandler(jimClientHandler);
    ChannelFuture channelFuture = null;
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    try {
      Bootstrap bootstrap = new Bootstrap();
      bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class).handler(jimClientInitializer);
      //绑定服务器
      channelFuture = bootstrap.connect(host, port).sync();

    } catch (Exception e) {
      e.printStackTrace();
      eventLoopGroup.shutdownGracefully();
    }
    return channelFuture;
  }


  public static void main(String[] args) {
    for(int i = 0;i<10;i++){
      JimClient j = new JimClient();
      j.connect().channel().writeAndFlush("select * from t"+i+" ; ");
    }
  }

}
