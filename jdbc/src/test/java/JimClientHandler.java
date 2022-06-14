import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import java.time.LocalDateTime;

public class JimClientHandler extends SimpleChannelInboundHandler<String> {

  private ChannelHandlerContext channelHandlerContext;

  public void writeMessage(String msg) throws Exception {
    this.channelHandlerContext.writeAndFlush(msg);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
    System.out.println("got from server ==>"+msg);

  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    channelHandlerContext = ctx;
    ctx.channel().writeAndFlush("来自客户端的问候！");
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.channel().close();
  }
}
