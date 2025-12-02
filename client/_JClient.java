import java.sql.*;
public class _JClient {
  public static void main(String[] args) throws Exception {
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test");
    Statement st = conn.createStatement();
    String sql = "SELECT id, name FROM user WHERE age >= 20 ORDER BY id DESC";
    System.out.println("SQL="+sql);
    ResultSet rs = st.executeQuery(sql);
    int n=0;
    while (rs.next()) {
      System.out.println(rs.getString("id") + "," + rs.getString("name"));
      n++;
    }
    System.out.println("N=" + n);
  }
}
