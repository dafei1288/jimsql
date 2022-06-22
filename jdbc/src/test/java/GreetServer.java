import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class GreetServer {
  private ServerSocket serverSocket;
  private Socket clientSocket;
  private PrintWriter out;
  private BufferedReader in;

  public void start(int port) throws Exception{
    serverSocket = new ServerSocket(port);
    clientSocket = serverSocket.accept();
    out = new PrintWriter(clientSocket.getOutputStream(), true);
    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    String greeting = in.readLine();
    if ("hello server".equals(greeting)) {
      out.println("hello client");
    }
    else {
      out.println("unrecognised greeting");
    }
  }

  public void stop() throws Exception{
    in.close();
    out.close();
    clientSocket.close();
    serverSocket.close();
  }
  public static void main(String[] args) throws Exception{
    GreetServer server=new GreetServer();
    server.start(6666);
  }
}