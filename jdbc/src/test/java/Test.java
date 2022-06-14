
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

  public static void main(String[] args) throws Exception {

//    System.setProperty("JIMSQL_DATADIR","D:\\working\\opensource\\jimsql\\jdbc\\src\\test\\resources\\");
//    File f  = new File("D:\\working\\opensource\\jimsql\\jdbc\\src\\test\\resources");
//    System.out.println("===>"+f.toPath().toUri());

//    String sql = "select * from user";
//    Pattern pattern = Pattern.compile("^select (\\*) from ([a-z]+)");
//    Matcher matcher = pattern.matcher(sql);
//    if(matcher.find()){
//      System.out.println(matcher.group(1));
//      System.out.println(matcher.group(2));
//    }


    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    Connection conn = DriverManager.getConnection("jdbc:jimsql:file:///D:/working/opensource/jimsql/jdbc/src/test/resources/");
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

