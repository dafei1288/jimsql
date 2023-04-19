import com.dafei1288.jimsql.common.Config;
import java.io.IOException;

public class TestConfig {

  public static void main(String[] args) throws IOException {
    Config config = Config.getConfig();
    System.out.println(config);
  }
}
