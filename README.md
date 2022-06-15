# What is Jim

JimSql = Jim Isn't MySQL. Jim is a filesystem database system implemention use Java.


# Useage

```java
public class Test {
  public static void main(String[] args) throws Exception {
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    Connection conn = DriverManager.getConnection("jdbc:jimsql:file:///D:/working/opensource/jimsql/jdbc/src/test/resources/");
    Statement statement = conn.createStatement();
    ResultSet resultSet = statement.executeQuery("select * from user");
    while(resultSet.next()){
      String id = resultSet.getString("id");
      String name = resultSet.getString("name");
      String age = resultSet.getString("age");
      System.out.println(String.format("row ==> id : %s , name : %s , age = %s",id,name,age));
    }
  }
}
```
