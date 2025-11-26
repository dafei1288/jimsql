# SQLPARSER TODO (MySQL dialect)

Scope
- File: server/src/main/resources/jimsql.g4
- Keep current rule names when possible to avoid breaking existing processors.

Goals
- Expand grammar to cover common MySQL syntax: SELECT/INSERT/UPDATE/DELETE, JOIN, WHERE/GROUP BY/HAVING/ORDER BY, LIMIT/OFFSET, UNION, strict CREATE TABLE types/params, ALTER TABLE, SHOW/DESC/EXPLAIN.
- Maintain backward compatibility with existing simple statements and parse-tree shapes where feasible.

Non-Goals (for now)
- Stored procedures, triggers, CTEs, window functions, prepared statements, views, partitions, complex index options.

Milestones & Tasks

1) Lexer & Basic Expressions
- [x] Comments and whitespace: support --, #, /* ... */ and hide all in HIDDEN channel
- [x] Literals: STRING_LITERAL (single-quote with escapes), INT_LITERAL, DECIMAL_LITERAL, TRUE/FALSE, NULL
- [x] Identifiers: backtick-quoted, double-quoted, and bare identifiers (QUOTED_ID + LETTERS); keep identifier entry rule
- [ ] Keywords: IF, EXISTS, NOT, IS, AS, DISTINCT, ORDER, BY, GROUP, HAVING, LIMIT, OFFSET, LIKE, IN, BETWEEN, JOIN, LEFT, RIGHT, FULL, OUTER, CROSS, ON, ASC, DESC, UNION, ALL, PRIMARY, UNIQUE, FOREIGN, REFERENCES, KEY, INDEX, CONSTRAINT
- [x] Expression precedence: arithmetic (+ - * / %), comparison (= != > >= < <=), special (IS [NOT] NULL, [NOT] IN, [NOT] LIKE, BETWEEN), logical (NOT/AND/OR), parentheses

2) SELECT Enhancements
- [x] SELECT [DISTINCT] selectList
- [x] FROM tableSource (table or subquery) with aliasing
- [x] JOIN: INNER/LEFT/RIGHT/FULL [OUTER]?/CROSS ... ON expr
- [x] WHERE, GROUP BY, HAVING
- [x] ORDER BY multiple items with ASC|DESC
- [x] LIMIT n [OFFSET m]
- [x] UNION / UNION ALL (basic chaining)

3) DML Complete
- [x] INSERT INTO t (cols)? VALUES rowValues (',' rowValues)*
- [x] INSERT ... SELECT (parse support)
- [x] INSERT ... SET col=expr (',' ...)*
- [ ] UPDATE t SET col=expr (',' ...)* WHERE ...
- [ ] DELETE FROM t WHERE ...

4) DDL (strict CREATE TABLE params) + DROP
- [ ] CREATE DATABASE [IF NOT EXISTS] db
- [ ] DROP DATABASE [IF EXISTS] db
- [ ] CREATE TABLE [IF NOT EXISTS] t '(' columnDef (',' columnDef)* (',' tableConstraint)* ')'
  - [ ] Column types and params (strict per "Type & Param Spec")
  - [ ] Column constraints: NULL/NOT NULL, DEFAULT literal, AUTO_INCREMENT, PRIMARY KEY, UNIQUE, COMMENT '...'
  - [ ] Table constraints: PRIMARY KEY(...), UNIQUE(...), [CONSTRAINT name]? FOREIGN KEY(... ) REFERENCES tbl(... )
- [ ] DROP TABLE [IF EXISTS] t

5) ALTER TABLE
- [ ] ALTER TABLE t ADD [COLUMN] columnDef [FIRST|AFTER col]?
- [ ] ALTER TABLE t DROP [COLUMN] col
- [ ] ALTER TABLE t MODIFY [COLUMN] columnDef
- [ ] ALTER TABLE t CHANGE [COLUMN] old_name new_name columnDef
- [ ] ALTER TABLE t RENAME [TO] new_table
- [ ] ALTER TABLE t ADD [UNIQUE] [INDEX|KEY] idx '(' colList ')'
- [ ] ALTER TABLE t DROP INDEX idx
- [ ] ALTER TABLE t ADD [CONSTRAINT name]? FOREIGN KEY(...) REFERENCES tbl(... )
- [ ] ALTER TABLE t DROP FOREIGN KEY fk_name
- [ ] ALTER TABLE t ADD/DROP PRIMARY KEY

6) DCL / SHOW / EXPLAIN
- [ ] SHOW DATABASES [LIKE pattern]?
- [ ] SHOW TABLES [LIKE pattern]?
- [ ] DESCRIBE | DESC tableName
- [ ] SHOW CREATE TABLE tableName
- [x] EXPLAIN select

7) Compatibility & Error Handling
- [ ] Preserve existing rule names (selectTable, columnList, tableName, identifier, etc.) and add subrules for new features
- [ ] Resolve lexer priority/ambiguities; extract subrules where needed
- [ ] Optional: add error listener for better diagnostics

8) Tests
- [ ] Add minimal success samples for each milestone
- [ ] Regression: existing simple SELECT/INSERT/UPDATE/DELETE/SHOW must still parse
- [ ] Edge cases: non-ASCII strings, backtick identifiers, case-mixed keywords, complex expressions

Type & Param Spec (CREATE TABLE strict)
- Integers: TINYINT | SMALLINT | MEDIUMINT | INT | BIGINT [UNSIGNED] (no display width)
- Floating: FLOAT | DOUBLE (no length); DECIMAL '(' p (',' s)? ')' (p required, s optional)
- Fixed/Var: CHAR '(' n ')', VARCHAR '(' n ')', BINARY '(' n ')', VARBINARY '(' n ')' (n required)
- Text/Binary: TINYTEXT | TEXT | MEDIUMTEXT | LONGTEXT | TINYBLOB | BLOB | MEDIUMBLOB | LONGBLOB (no length)
- Datetime: DATE | TIME | DATETIME | TIMESTAMP | YEAR
- Misc: JSON | BOOL | BOOLEAN
- ENUM/SET: ENUM '(' stringList ')' | SET '(' stringList ')' (at least 1 value)
- Column constraints: NULL/NOT NULL, DEFAULT literal, AUTO_INCREMENT, PRIMARY KEY, UNIQUE, COMMENT '...'
- Table constraints: PRIMARY KEY(colList), UNIQUE(colList), [CONSTRAINT name]? FOREIGN KEY(colList) REFERENCES tbl(colList)

Acceptance Criteria
- All milestone examples parse; no regressions on existing statements
- Representative examples supported:
  - SELECT with JOIN/WHERE/GROUP/HAVING/ORDER/LIMIT/UNION
  - INSERT with multi-row VALUES, SELECT source, SET form
  - UPDATE/DELETE with WHERE
  - CREATE TABLE with strict types and constraints; table-level PK/FK
  - ALTER TABLE add/modify/change/drop/rename; index/pk/fk management
  - SHOW/DESC/EXPLAIN variants

Examples (should parse)

```sql
CREATE TABLE `user` (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  age INT,
  balance DECIMAL(18,2) DEFAULT 0,
  bio TEXT,
  status ENUM('A','I') NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (id)
);
```

```sql
ALTER TABLE `user` ADD COLUMN nick VARCHAR(64) AFTER name;
ALTER TABLE `user` MODIFY COLUMN balance DECIMAL(18,2) NOT NULL DEFAULT 0;
ALTER TABLE `user` ADD INDEX idx_name (name);
ALTER TABLE `user` DROP INDEX idx_name;
ALTER TABLE `user` RENAME TO `user_v2`;
```

```sql
SELECT DISTINCT u.id, u.name, SUM(o.amount) AS total
FROM `user` u
LEFT JOIN `order` o ON o.user_id = u.id
WHERE u.status = 'A' AND o.created_at >= '2024-01-01'
GROUP BY u.id, u.name
HAVING SUM(o.amount) > 100
ORDER BY total DESC, u.id ASC
LIMIT 10 OFFSET 20;
```

```sql
SELECT id, name FROM a
UNION ALL
SELECT id, name FROM b;
```

Next Steps (planned)\n- INSERT ... SELECT / INSERT ... SET grammar (no execution changes)\n- DDL strict CREATE TABLE and ALTER TABLE family\n- SHOW CREATE TABLE / DESCRIBE variants\n- Tests under server/src/test/java for new syntax (no runtime changes)\n


Execution Plan (Minimal)
- [x] 1) ORDER BY, LIMIT/OFFSET: implemented in QueryPhysicalPlan; applies when all ORDER BY columns exist in current table; stable for single-table queries.
- [x] 2) WHERE (simple AND of column comparisons): implemented in QueryPhysicalPlan; supports =, !=, >, >=, <, <= with numeric/string literals; applies only when all referenced columns exist in current table; no OR/parentheses/functions yet.
- [ ] 3) GROUP BY/HAVING (basic aggregates): planned; COUNT/SUM/AVG only.
- [ ] 4) JOIN (INNER, eq-join): planned; nested loop on single ON equality.

Notes
- Sorting and filtering operate on CSV-backed table rows; types derived from ServerMetadata (header + sample row).
- For JOIN queries, execution still uses single-table path; filters requiring other tables are skipped to avoid false negatives.