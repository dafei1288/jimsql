# PR Title

<!-- e.g. feat(server/dml): add UPDATE/DELETE executor for CSV -->

## Purpose
- What problem does this PR solve? Why now?

## Changes
- Modules and key files touched
- Brief description of approach (parser/executor/protocol/JDBC/etc.)

## Run / Verify Steps
- Build
  - `mvn -q -DskipTests package`
- Server
  - legacy: `java -jar server/target/*-jar-with-dependencies.jar 8821 0.0.0.0 server/src/main/resources/datadir`
  - jspv1: `java -jar server/target/*-jar-with-dependencies.jar 8821 0.0.0.0 server/src/main/resources/datadir jimsql.protocol=jspv1`
- JDBC
  - legacy: `jdbc:jimsql://127.0.0.1:8821/<db>?protocol=legacy`
  - jspv1: `jdbc:jimsql://127.0.0.1:8821/<db>?protocol=jspv1`
- Tests
  - All: `mvn test` or per module `mvn -pl <module> test`
  - (optional) attach logs/screenshots for server/Docker changes

## Breaking Changes
- Any API/protocol/sql syntax changes? default behavior changes?

## Linked Issues
- Closes #<id>

## Screenshots / Logs
- (attach when server/Docker behavior affected)

## Checklist
- [ ] Build passes: `mvn -q -DskipTests package`
- [ ] Server boots (legacy / jspv1 as applicable)
- [ ] Protocol behavior verified (legacy=OK/FINISH; jspv1=UPDATE_COUNT/RESULTSET)
- [ ] Docs updated (README / docs/* / AGENTS.md)
- [ ] SQLPARSER_TODO.md updated (if grammar/features changed)
- [ ] No unrelated files mixed in this PR
- [ ] Regression checked (simple SELECT/INSERT/UPDATE/DELETE/SHOW parses)

## Risk & Mitigations
- Potential impacts and how to roll back

