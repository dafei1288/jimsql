# JimSQL是什么

JimSql = Jim Isn't MySQL. JimSQL是纯 java 编写的数据库系统.

# 如何使用JimSQL

使用 `docker-compose` 尝试，配置文件如下
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


添加snapshots仓库
```xml
  <repositories>
  <repository>
    <id>jim</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
  </repository>
</repositories>
```

引入jdbc依赖
```xml
<dependency>
  <groupId>com.dafei1288</groupId>
  <artifactId>jdbc</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```
示例代码：

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

# 如何开发

## 环境要求
1. Java >= 17
2. Maven >= 3.8 

## 如何从代码编译
`mvn clean package -Ddockerfile.skip -DskipTest=true`