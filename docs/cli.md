# JimSQL CLI

JimSQL 的命令行客户端（Java 17+），支持交互式 REPL、单次执行、CSV/JSON 导入/导出与彩色输出。

## 安装与构建

- 前置：JDK 17+，Maven 3.8+
- 构建（仅 CLI）：
  - `mvn -pl cli -am -DskipTests package`
  - 产物：`cli/target/cli-1.0.0-SNAPSHOT.jar`

> 注意：首次构建会下载依赖，需要网络访问。

## 快速开始 (也可使用包装脚本 cli/bin/jimsql(.bat))

- 启动服务端（示例）
  - `java -jar server/target/server-1.0.0-SNAPSHOT-jar-with-dependencies.jar 8821 0.0.0.0 server/src/main/resources/datadir`
- 执行一次查询
  - `java -jar cli/target/cli-1.0.0-SNAPSHOT.jar --url jdbc:jimsql://127.0.0.1:8821/test -c "select * from user"`
- 进入 REPL（交互）
  - `java -jar cli/target/cli-1.0.0-SNAPSHOT.jar --url jdbc:jimsql://127.0.0.1:8821/test`

> REPL 会尝试使用提供的 URL（或环境变量，见下文）自动连接。

## 连接配置

- 支持参数：
  - `--url` 或 `-H/--host`、`-p/--port`、`-d/--db`、`-u/--user`、`-P/--password`
  - 例：`-H 127.0.0.1 -p 8821 -d test`
- 支持环境变量（与 MCP 保持一致）：
  - 首选 `JIMSQL_URL`，否则使用 `JIMSQL_HOST`/`JIMSQL_PORT`/`JIMSQL_DB`/`JIMSQL_USER`/`JIMSQL_PASSWORD`
- 协议（可选）：
  - 通过 URL 查询参数指定：`jdbc:jimsql://host:port/db?protocol=legacy` 或 `?protocol=jspv1`
  - 或 JVM 参数：`-Djimsql.protocol=legacy|jspv1`

## 命令用法

- 查看帮助：
  - `java -jar cli/target/cli-1.0.0-SNAPSHOT.jar --help`
- 非交互执行：
  - `-c "SQL"`、`-f file.sql`、或通过 stdin 管道
- 输出格式：
  - `--format table|csv|json`（默认 table）
  - 彩色输出（表格）：`--color always|auto|never`（默认 always）
  - 提示：`NO_COLOR` 环境变量将强制关闭颜色

## REPL 元命令

- `\q` / `\quit` / `\exit`：退出
- `\help`：帮助
- `\timing on|off`：切换耗时显示（默认 on）
- `\set format table|csv|json`：切换输出格式
- `\connect <jdbc-url>`：切换连接
- `\import csv|json <file> <table>`：导入
- `\export csv|json <table> <file>`：导出

## 导入/导出

- 导入 CSV：`--import csv --into <table> --in <file>`（CSV 首行作为列名）
- 导入 JSON：`--import json --into <table> --in <file>`（`[{col:val,...},...]`）
- 导出 CSV：`--export csv --table <table> --out <file>`
- 导出 JSON：`--export json --table <table> --out <file>` 或 `--query "SELECT ..."`

> 当前仓库 JDBC/Server 对 DML 与 PreparedStatement 的支持仍在演进中：
> - `executeQuery` 已对接；`executeUpdate`/`prepareStatement` 多数未实现。
> - 导入（INSERT 批量）在驱动完善前可能不可用；导出依赖查询能力。
> 如遇不支持，请先通过 REPL/单次查询验证 SELECT 能力，再尝试导出。

## 示例

- 表格输出（彩色）：
  - `... -c "select id,name,age from user order by id"`
- CSV：
  - `type query.sql | ... --format csv > out.csv`
- JSON：
  - `... -c "select * from user" --format json > out.json`
- 导出整表：
  - `... --export csv --table user --out user.csv`

## 常见问题

- `--help` 无效或与 `-h` 冲突：已将主机短参改为 `-H`；使用 `--help` 或 `-h` 查看帮助。
- Windows 控制台颜色：已集成 Jansi；如输出包含 `\u001B[...]`，请加 `--color never`。
- 协议不匹配（Bad magic 等）：请在 URL 加 `?protocol=legacy` 或 `?protocol=jspv1` 再试。
- 连接但查询无结果：当前驱动 `Statement.execute` 未返回结果集，请使用本 CLI（已自动判断 SELECT 使用 `executeQuery`）。

## 版本与发布

- Jar：`cli/target/cli-<ver>.jar`
- 后续可选：提供 Windows `.bat`、Unix shell 包装脚本；或使用 jlink 生成小体积运行时。

## 包装脚本
- Windows: cli\bin\jimsql.bat
- Bash/WSL/Linux/Mac: cli/bin/jimsql (首次需 chmod +x)

示例:
- cli\bin\jimsql --help
- cli\bin\jimsql --url jdbc:jimsql://127.0.0.1:8821/test -c "select 1"
