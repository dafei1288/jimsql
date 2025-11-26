# Repository Guidelines

## Project Structure & Modules
- Maven multi-module layout:
  - `common/` — shared models, metadata, and utilities.
  - `server/` — Netty-based JimSQL server; Dockerfile and compose in `server/`.
  - `jdbc/` — JDBC driver (`com.dafei1288.jimsql.jdbc`).
  - `jimsql_mcp_server/` — Spring Boot MCP integration.
- Standard layout: `src/main/java`, `src/main/resources`, tests in `src/test/java`.

## Build, Test, and Development Commands
- Build all modules: `mvn clean install -DskipTests`
- Build a module (with deps): `mvn -pl server -am package -DskipTests`
- Run server (local JAR): `java -jar server/target/*-jar-with-dependencies.jar 8821 0.0.0.0 server/src/main/resources/datadir`
- Run with Docker: `docker compose -f server/docker-compose.yml up -d`
- Run tests: `mvn test` (per module: `mvn -pl jdbc test`)
- CI builds on JDK 17 with `mvn -B package -DskipTests=true` and publishes artifacts and Docker images.

## Coding Style & Naming Conventions
- Java 17+ (CI uses 17). Ensure `JAVA_HOME` points to a 17-compatible JDK.
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
- Docker env: set `JAVA_ARGS` (e.g., `8821 0.0.0.0 /jimsql/data`) and optional `JAVA_OPTS` for JVM flags.


## MCP Startup & Connection Configuration

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
