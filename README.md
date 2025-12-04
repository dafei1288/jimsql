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

Use snapshot  repository

```xml
  <repositories>
  <repository>
    <id>jim</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
  </repository>
</repositories>
```

Then use jdbc to connect

```xml
<dependency>
  <groupId>com.dafei1288</groupId>
  <artifactId>jdbc</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```java
import java.sql.*;

public class TestServer {
  public static void main(String[] args) throws Exception {
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    // legacy: OK only for DML; jspv1: returns UPDATE_COUNT
    Connection conn = DriverManager.getConnection("jdbc:jimsql://localhost:8821/test?protocol=jspv1");
    Statement stmt = conn.createStatement();

    // SELECT
    try (ResultSet rs = stmt.executeQuery("select id,name,age from user where age >= 3 order by id desc")) {
      while (rs.next()) {
        System.out.printf("id=%s name=%s age=%s%n", rs.getString("id"), rs.getString("name"), rs.getString("age"));
      }
    }

    // UPDATE (jspv1 only returns affected rows)
    int n1 = stmt.executeUpdate("UPDATE test.user SET age = 23 WHERE id = 1");
    // DELETE
    int n2 = stmt.executeUpdate("DELETE FROM test.user WHERE id = 3");
    System.out.printf("updated=%d deleted=%d%n", n1, n2);
  }
}
```

See more examples in `docs/dml-examples.md`.

## WHERE Filtering

- AND/OR/NOT with parentheses; comparisons `= != > >= < <=`; `IS NULL`/`IS NOT NULL`; `LIKE`/`NOT LIKE`; `IN`/`NOT IN`.
- Case-insensitive column names; numeric vs string comparison based on table metadata.
- CSV NULL semantics: empty string is treated as NULL for `IS NULL` checks.

Examples:
```sql
SELECT id,name,age FROM user WHERE age = 3 OR (age = 22 AND name LIKE 'ja%') ORDER BY id DESC;
SELECT id,name FROM user WHERE name IN ('jacky','doudou') AND age IS NOT NULL;
```