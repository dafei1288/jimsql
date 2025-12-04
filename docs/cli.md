# JimSQL CLI

交互式/命令行客户端，基于 Java 21+。支持单条命令执行与 REPL，适配 legacy/jspv1 两种协议，适用于 CSV 数据后端。

## 环境要求
- Java 21+
- Maven 3.8+

## 编译
```bash
mvn -pl cli -am -DskipTests package
```
构建产物：`cli/target/cli-1.0.0-SNAPSHOT.jar`

## 启动服务端（本地）
- 默认（legacy）：
```bash
java -jar server/target/*-jar-with-dependencies.jar 8821 0.0.0.0 server/src/main/resources/datadir
```
- jspv1：
```bash
java -jar server/target/*-jar-with-dependencies.jar 8821 0.0.0.0 server/src/main/resources/datadir jspv1
```
- Windows 注意：`server/bin/start-server.cmd` 已正确处理 DATADIR 的引号；传参时无需额外转义。

## 运行 CLI
- 单条命令：
```bash
java -jar cli/target/cli-1.0.0-SNAPSHOT.jar --url "jdbc:jimsql://127.0.0.1:8821/test" -c "show databases;"
```
- 进入 REPL：
```bash
java -jar cli/target/cli-1.0.0-SNAPSHOT.jar --url "jdbc:jimsql://127.0.0.1:8821/test"
```
- 脚本：
  - Windows: `cli\\bin\\jimsql.bat`
  - Bash/WSL/Linux/Mac: `cli/bin/jimsql`（需要 `chmod +x`）

## 连接参数
- 使用 `--url`（推荐）或 `-H/--host`、`-p/--port`、`-d/--db`、`-u/--user`、`-P/--password`
- 协议：在 JDBC URL 上追加 `?protocol=legacy|jspv1`
  - 例如：`jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1`

## 输出格式与颜色
- `--format table|csv|json`，默认 `table`
- 颜色：`--color always|auto|never`，支持 `NO_COLOR` 环境变量屏蔽颜色

## REPL 命令
- `\q` / `\quit` / `\exit` 退出
- `\help` 帮助
- `\timing on|off` 显示/关闭执行耗时
- `\set format table|csv|json` 调整输出格式
- `\connect <jdbc-url>` 切换连接
- `\import csv|json <file> <table>` 导入数据
- `\export csv|json <table> <file>` 导出数据，支持 `--query "SELECT ..."`

## DML/协议提示
- PreparedStatement/executeQuery/executeUpdate 语义与 JDBC 保持一致；
- legacy：DML 返回 OK；jspv1：返回 UPDATE_COUNT（影响行数）。

## 常见问题
- Windows 终端颜色异常：可用 `--color never` 或配置支持 ANSI 的终端。
- 协议不匹配（如 "Bad magic"）：请在 URL 指定 `?protocol=legacy|jspv1`。
- SLF4J StaticLoggerBinder 提示：server/cli 已引入 `slf4j-nop`，不会再输出该警告。

## 示例
```bash
# 单条命令
java -jar cli/target/cli-1.0.0-SNAPSHOT.jar --url "jdbc:jimsql://127.0.0.1:8821/test" -c "select id,name,age from user order by id;"

# REPL
java -jar cli/target/cli-1.0.0-SNAPSHOT.jar --url "jdbc:jimsql://127.0.0.1:8821/test"
# 在 REPL 内：
-- show tables;
-- select * from user where age >= 3 and name like 'j%';
```