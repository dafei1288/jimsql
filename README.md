# What is Jim

JimSql = Jim Isn't MySQL. Jim is a filesystem database system implemention use Java.

# Useage

use `docker-compose` to start a server
```yaml
version: '3'
services:
  jimsql:
    privileged: true
    image: dafei1288/jimsql_server:1.0.0
    ports:
      - "8821:8821"
      - "8825:8825"
#     volumes:
#       - "./data:/jimsql/data"
    environment:
#      JAVA_OPTS: -agentlib:jdwp=transport=dt_socket,address=*:8825,server=y,suspend=n
      JAVA_ARGS: 8821 0.0.0.0 /jimsql/data
```

Then use jdbc to connect

```xml
<dependency>
  <groupId>com.dafei1288.jimsql</groupId>
  <artifactId>jdbc</artifactId>
  <version>${jimsql.version}</version>
</dependency>
```


```java
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
    String sql = "select id,name from user";
    ResultSet resultSet = statement.executeQuery(sql);
    System.out.println(sql);
    System.out.println(resultSet);
    while(resultSet.next()){

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<String> colNames = new ArrayList<>();
      for(int i=0;i<resultSetMetaData.getColumnCount();i++){
        String colName = resultSetMetaData.getColumnLabel(i);
        System.out.println(colName+" => "+resultSet.getString(colName));
      }
      System.out.println();
    }

  }
}
```
