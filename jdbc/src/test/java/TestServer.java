
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TestServer {

  public static void main(String[] args) throws Exception {



    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    Connection conn = DriverManager.getConnection("jdbc:jimsql://localhost:8821/test?protocol=jspv1");
//      -Djimsql.protocol=jspv1  &&wirelog=json
//    Connection conn = DriverManager.getConnection("jdbc:jimsql://localhost:8821/test?protocol=legacy");

    System.out.println(conn);

    Statement statement = conn.createStatement();
    System.out.println(statement);
    String sql = "select id,name,age from user where age = 3";
    sql = "show tables ;";
    ResultSet resultSet = statement.executeQuery(sql);
    System.out.println(sql);
    System.out.println(resultSet);
    while(resultSet.next()){
//      id,name,age
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<String> colNames = new ArrayList<>();
      int count = resultSetMetaData.getColumnCount();
      for (int i = 1; i <= count; i++) { // JDBC column index is 1-based
        String colName = resultSetMetaData.getColumnLabel(i);
        System.out.println(colName + " => " + resultSet.getString(colName));
      }
      System.out.println();
//      String id = resultSet.getString("id");
//      String name = resultSet.getString("name");
//      String age = resultSet.getString("age");
//      System.out.println(String.format("row ==> id : %s , name : %s , age = %s",id,name,age));
    }

  }
}

