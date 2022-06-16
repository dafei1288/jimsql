
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
    Connection conn = DriverManager.getConnection("jdbc:jimsql://localhost:8821/test");
    System.out.println(conn);

    Statement statement = conn.createStatement();
    System.out.println(statement);

    ResultSet resultSet = statement.executeQuery("select * from user");
    System.out.println(resultSet);
    while(resultSet.next()){
//      id,name,age
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<String> colNames = new ArrayList<>();
      for(int i=0;i<resultSetMetaData.getColumnCount();i++){
//        colNames.add(resultSetMetaData.getColumnName(i));
        String colName = resultSetMetaData.getColumnLabel(i);
        System.out.println(colName+" => "+resultSet.getString(colName));
      }
      System.out.println();
//      String id = resultSet.getString("id");
//      String name = resultSet.getString("name");
//      String age = resultSet.getString("age");
//      System.out.println(String.format("row ==> id : %s , name : %s , age = %s",id,name,age));
    }

  }
}

