# DML Examples (UPDATE / DELETE)

This doc shows how to use UPDATE/DELETE against JimSQL's CSV backend in both protocols.

## Data

Default demo data is under `server/src/main/resources/datadir/test` (e.g. `user.csv`).

## Legacy Protocol

- Start server (legacy is default):
  - `java -jar server/target/*-jar-with-dependencies.jar 8821 0.0.0.0 server/src/main/resources/datadir`
- Connect via JDBC (legacy):
  - `jdbc:jimsql://127.0.0.1:8821/test?protocol=legacy`
- Execute DML:
  - `UPDATE test.user SET age = 23 WHERE id = 1;` -> server returns `OK` (no row count)
  - `DELETE FROM test.user WHERE id = 3;` -> server returns `OK`
- Verify with SELECT.

## JSPv1 Protocol (row-binary + UPDATE_COUNT)

- Start server with protocol:
  - `java -jar server/target/*-jar-with-dependencies.jar 8821 0.0.0.0 server/src/main/resources/datadir jimsql.protocol=jspv1`
- Connect via JDBC:
  - `jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1`
- Execute DML:
  - `int n = stmt.executeUpdate("UPDATE test.user SET age = 23 WHERE id = 1"); // n == affected rows`
  - `int n = stmt.executeUpdate("DELETE FROM test.user WHERE id = 3");`
- Verify with SELECT.

## WHERE Semantics (current)

- Supported: `AND` conjunctions of simple comparisons on a single table:
  - operators: `= != > >= < <=`
  - literals: numbers, single-quoted strings
  - column names are case-insensitive
- Not yet supported: `OR`, parentheses, functions.

## Notes

- CSV is not escaped; string values are written as-is.
- `NULL/TRUE/FALSE` are written as plain text.
- For multi-table queries and advanced predicates: not supported in DML executor.
