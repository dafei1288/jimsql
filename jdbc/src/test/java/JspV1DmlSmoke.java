import java.sql.*;

public class JspV1DmlSmoke {
  public static void main(String[] args) throws Exception {
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1")) {
      try (Statement st = conn.createStatement()) {
        int u = st.executeUpdate("UPDATE test.user SET age = 99 WHERE id = 2");
        int d = st.executeUpdate("DELETE FROM test.user WHERE id = 99999"); // likely 0
        System.out.println("update="+u+" delete="+d);
      }
    }
  }
}
