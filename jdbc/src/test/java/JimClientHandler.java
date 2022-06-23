import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import java.time.LocalDateTime;

public class JimClientHandler extends SimpleChannelInboundHandler<Object> {

  private ChannelHandlerContext channelHandlerContext;

  public void writeMessage(String msg) throws Exception {
    this.channelHandlerContext.writeAndFlush(msg);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

    System.out.println("got from server ==>"+msg+" =>"+this.hashCode());
    ctx.writeAndFlush("next");

  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    super.channelReadComplete(ctx);
    System.out.println("完成。。。");
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
