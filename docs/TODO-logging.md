# Logging System TODO (Server/CLI/JDBC)

Goal: Replace ad-hoc System.out debug with a consistent, configurable logging system across modules (server, cli, jdbc). Silence binder warnings, provide levels, and support file rotation.

Must-have
- Facade: use SLF4J API everywhere (no System.out/err in core paths); minimal wrappers for Netty/JDBC wire where needed.
- Binding per runtime:
  - server: logback-classic (or log4j2) as default; config via `logback.xml`/`log4j2.xml` in classpath.
  - cli: slf4j-nop by default to stay quiet; add `--verbose/--log-level` to enable console logs.
  - jdbc: slf4j-nop (quiet by default); optional DEBUG via system property for troubleshooting.
- Levels & categories: DEBUG for parser/executor (join/where/agg), INFO for startup, WARN/ERROR for failures.
- Config knobs: env and `-D` flags (e.g., `JIMSQL_LOG_LEVEL`, `-Djimsql.log.level=DEBUG`), Docker env passthrough.
- Output: console pattern + rolling file appender (`logs/server.log`), size/time rotation, retain N files.
- Windows: ensure paths and encodings OK; avoid BOM; no blocking UI dialogs.
- Docs: README “Logging” section; examples for enabling DEBUG, sample config.

Nice-to-have
- Structured logging option (JSON layout) guarded by config.
- Per-package overrides (e.g., `com.dafei1288.jimsql.server.plan=DEBUG`).
- Netty logger bridge to SLF4J; JDBC wire timing at TRACE.
- MDC context (connection id, client addr) for server requests.
- Docker image mounts `/jimsql/logs`; compose example includes log volume.

Migration Plan
1) Add slf4j-api to all modules; server binds to logback (runtime); cli/jdbc bind to slf4j-nop by default.
2) Introduce `Logger` instances in hot paths (parser/executor/handler); replace `System.out.println` with `log.debug/info/warn/error`.
3) Add default `logback.xml` for server (console INFO + rolling file INFO); CLI gains `--log-level` and respects `JIMSQL_LOG_LEVEL`.
4) Document usage in README/AGENTS.md; add examples for Windows/Linux.
5) Remove temporary debug prints from QueryPhysicalPlan and friends once JOIN is verified.
6) CI: run with `-Djimsql.log.level=WARN` to keep logs minimal; ensure no `StaticLoggerBinder` warnings.

Risks
- Log noise/regression if default levels too verbose: default INFO for server; CLI/JDBC default NOP.
- Classpath conflicts with existing dependencies: pin SLF4J/logback versions; exclude duplicate bindings.

Tracking
- Issue: “Design and integrate logging system (SLF4J + logback)” (to be opened)
- Related: silence SLF4J binder warnings noted during server/cli startup.
