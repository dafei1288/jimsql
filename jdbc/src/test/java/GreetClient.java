


import com.dafei1288.jimsql.common.JimSQueryStatus;
import io.netty.handler.codec.serialization.ObjectDecoderInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;
import com.dafei1288.jimsql.common.RowData;
import java.util.ArrayList;
import java.util.List;

public class GreetClient {
  private Socket clientSocket;
  private PrintWriter out;
  private InputStream in;

  public void startConnection(String ip, int port) throws IOException {
    clientSocket = new Socket(ip, port);
    out = new PrintWriter(clientSocket.getOutputStream(), true);
//    in = clientSocket.getChannel().read()
//    in = new InputStreamReader(clientSocket.getInputStream());
//    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    in = clientSocket.getInputStream();
  }

  public String sendMessage(String msg) throws Exception {
    out.println(msg);
    ObjectDecoderInputStream decoderInputStream = new ObjectDecoderInputStream(in);
    List<RowData> datas = new ArrayList<>();
    boolean tag = true;
    while (tag){
      Object obj = decoderInputStream.readObject();
      if(obj instanceof RowData){
        datas.add((RowData) obj);
      }
      if(obj instanceof JimSQueryStatus && JimSQueryStatus.FINISH.equals((JimSQueryStatus)obj)){
        tag = false;
      }
//      System.out.println("===> "+decoderInputStream.readObject().toString());
    }
    System.out.println(datas);
    return "ok";

  }



  public void stopConnection() throws IOException {
    in.close();
    out.close();
    clientSocket.close();
  }

  public static void main(String[] args) throws Exception {
    GreetClient gc = new GreetClient();
    gc.startConnection("localhost",9008);

    String str = gc.sendMessage("select * from 1");
    System.out.println(str);
//    gc.stopConnection();id
  }

}