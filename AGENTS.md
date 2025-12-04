# Repository Guidelines

## Project Structure & Modules
- Maven multi-module layout:
  - `common/` ??shared models, metadata, and utilities.
  - `server/` ??Netty-based JimSQL server; Dockerfile and compose in `server/`.
  - `jdbc/` ??JDBC driver (`com.dafei1288.jimsql.jdbc`).
  - `jimsql_mcp_server/` ??Spring Boot MCP integration.
- Standard layout: `src/main/java`, `src/main/resources`, tests in `src/test/java`.

## Build, Test, and Development Commands
- Build all modules: `mvn clean install -DskipTests`
- Build a module (with deps): `mvn -pl server -am package -DskipTests`
- Run server (local JAR): `java -jar server/target/*-jar-with-dependencies.jar 8821 0.0.0.0 server/src/main/resources/datadir`
- Run with Docker: `docker compose -f server/docker-compose.yml up -d`
- Run tests: `mvn test` (per module: `mvn -pl jdbc test`)
- CI builds on JDK 21 with `mvn -B package -DskipTests=true` and publishes artifacts and Docker images.

- DML supported: UPDATE/DELETE (CSV backend)\n  - legacy protocol: returns OK for non-query\n  - jspv1 protocol: returns UPDATE_COUNT with affected rows\n\n## Coding Style & Naming Conventions
- DCL/SHOW supported: SHOW DATABASES, SHOW TABLES, DESCRIBE/DESC, SHOW CREATE TABLE (same columns across legacy/jspv1)\n  - SHOW DATABASES: `database`\n  - SHOW TABLES: `table`\n  - DESCRIBE/DESC t: `Field`, `Type`\n  - SHOW CREATE TABLE t: `Table`, `Create Table`\n- Java 21+ (CI uses 21). Ensure `JAVA_HOME` points to JDK 21.
- Indentation: 2 spaces; K&R braces; keep diffs small and focused.
- Packages lower-case under `com.dafei1288.jimsql.*`; classes `PascalCase`; constants `UPPER_SNAKE_CASE`.
- No formatter configured in Maven; align with surrounding code.

## Testing Guidelines
- Place tests under `src/test/java` mirroring package structure.
- Use JUnit; prefer `*Test.java` naming and fast unit tests in `common`/`jdbc`.
- Add integration tests for `server` (Netty) where practical.
- Run all tests: `mvn -DskipTests=false test`. No strict coverage gate; aim for meaningful coverage.

## Commit & Pull Request Guidelines
- Commits are short and imperative (e.g., `update maven.yml`); Conventional Commits welcome but not required.
- PRs should include: purpose, affected modules, run/test steps, breaking changes, and linked issues.
- Provide logs or screenshots for server/Docker changes. Ensure `mvn -q -DskipTests package` passes and server still boots.

## Security & Configuration Tips
- Default ports: `8821` (server), `8825` (debug). Default data dir: `server/src/main/resources/datadir`.
- Docker env: set `JAVA_ARGS` (e.\n- Windows: server/bin/start-server.cmd handles DATADIR quoting correctly; pass the directory without extra escaping.g., `8821 0.0.0.0 /jimsql/data`) and optional `JAVA_OPTS` for JVM flags.


## WHERE Filtering (SELECT)
- Supported (this branch): AND/OR/NOT with parentheses; comparisons `= != > >= < <=`; `IS NULL`/`IS NOT NULL`; `LIKE`/`NOT LIKE` (`%`/`_`); `IN`/`NOT IN` (numbers or single-quoted strings).
- Column matching is case-insensitive; identifiers may be quoted with backticks/double quotes or qualified (db.table.col); evaluation normalizes to column name.
- Numeric vs string comparison: decided by table metadata (numeric types use BigDecimal; others use string compare).
- NULL semantics (CSV backend): empty string is treated as NULL for `IS NULL`/`IS NOT NULL` checks.
- LIKE is case-sensitive; `%` matches any length, `_` matches a single character.

Examples:
```sql
-- OR + parentheses
SELECT id,name,age FROM user WHERE age = 3 OR (age = 22 AND name LIKE 'ja%');

-- IN + IS NOT NULL
SELECT id,name FROM user WHERE name IN ('jacky','doudou') AND age IS NOT NULL;

-- NOT + LIKE
SELECT * FROM user WHERE NOT (name LIKE '%test%' OR name IS NULL);
```## MCP Startup & Connection Configuration

- The MCP server reads JimSQL connection from env: prefer `JIMSQL_URL`, otherwise `JIMSQL_HOST`/`JIMSQL_PORT`/`JIMSQL_DB`/`JIMSQL_USER`/`JIMSQL_PASSWORD`.
- Enable stdio mode by setting `MCP_STDIO=true` or passing JVM arg `-Dmcp.stdio=true`.

Quick start (choose one):

- Windows PowerShell (stdio, granular env)

```powershell
$env:JIMSQL_HOST="127.0.0.1"
$env:JIMSQL_PORT="8821"
$env:JIMSQL_DB="test"
$env:JIMSQL_USER=""
$env:JIMSQL_PASSWORD=""
$env:MCP_STDIO="true"
java -jar jimsql_mcp_server\target\jimsql_mcp_server-3.3.9.jar
```

- Windows PowerShell (stdio, single URL)

```powershell
$env:JIMSQL_URL="jdbc:jimsql://127.0.0.1:8821/test"
$env:MCP_STDIO="true"
java -jar jimsql_mcp_server\target\jimsql_mcp_server-3.3.9.jar
```

- Bash (stdio)

```bash
JIMSQL_URL="jdbc:jimsql://127.0.0.1:8821/test" MCP_STDIO=true java -jar jimsql_mcp_server/target/jimsql_mcp_server-3.3.9.jar
```

Example MCP client config (env + args):

```json
{
  "mcpServers": {
    "jimsql": {
      "command": "java",
      "args": ["-Dmcp.stdio=true", "-jar", "jimsql_mcp_server/target/jimsql_mcp_server-3.3.9.jar"],
      "env": {
        "JIMSQL_HOST": "127.0.0.1",
        "JIMSQL_PORT": "8821",
        "JIMSQL_DB": "test",
        "JIMSQL_USER": "",
        "JIMSQL_PASSWORD": ""
        // or use only: "JIMSQL_URL": "jdbc:jimsql://127.0.0.1:8821/test"
      }
    }
  }
}
```

Notes:
- Connection details are read from env only; `-D` flags are used only for `mcp.stdio`. If desired, we can extend the code to also read `-Djimsql.*`.

