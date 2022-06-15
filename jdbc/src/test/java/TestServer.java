
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestServer {

  public static void main(String[] args) throws Exception {



    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    Connection conn = DriverManager.getConnection("jdbc:jimsql://localhost:9008/test");
    System.out.println(conn);

    Statement statement = conn.createStatement();
    System.out.println(statement);

    ResultSet resultSet = statement.executeQuery("select * from user");
    System.out.println(resultSet);
    while(resultSet.next()){
//      id,name,age
      String id = resultSet.getString("id");
      String name = resultSet.getString("name");
      String age = resultSet.getString("age");
      System.out.println(String.format("row ==> id : %s , name : %s , age = %s",id,name,age));
    }

  }
}

