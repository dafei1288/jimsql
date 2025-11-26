package com.dafei1288.jimsql.server.parser;

import org.snt.inmemantlr.GenericParser;
import org.snt.inmemantlr.comp.DefaultCompilerOptionsProvider;
import org.snt.inmemantlr.listener.DefaultTreeListener;
import org.snt.inmemantlr.exceptions.*;

import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Parser smoke tests (no JUnit dependency).
 *
 * How to run manually (after building the module):
 *   java -cp "target/test-classes;target/classes;target/dependency/*" com.dafei1288.jimsql.server.parser.SqlParserSmokeTest
 *
 * This class compiles jimsql.g4 with inmemantlr and parses representative SQLs.
 */
public class SqlParserSmokeTest {

  public static void main(String[] args) throws Exception {
    String grammar = loadGrammar();
    GenericParser gp = new GenericParser(grammar);
    DefaultCompilerOptionsProvider cop = new DefaultCompilerOptionsProvider();
    cop.getOptions().clear();
    cop.getOptions().add("-source");
    cop.getOptions().add("8");
    cop.getOptions().add("-target");
    cop.getOptions().add("8");
    gp.setCompilerOptionsProvider(cop);

    DefaultTreeListener l = new DefaultTreeListener();
    gp.setListener(l);
    gp.compile();

    String[] sqls = new String[] {
      // CREATE TABLE strict
      "CREATE TABLE `user` (\n" +
      "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
      "  name VARCHAR(100) NOT NULL,\n" +
      "  age INT,\n" +
      "  balance DECIMAL(18,2) DEFAULT 0,\n" +
      "  bio TEXT,\n" +
      "  status ENUM('A','I') NOT NULL,\n" +
      "  created_at DATETIME NOT NULL,\n" +
      "  PRIMARY KEY (id)\n" +
      ")",

      // ALTER TABLE variants
      "ALTER TABLE `user` ADD COLUMN nick VARCHAR(64) AFTER name",
      "ALTER TABLE `user` MODIFY COLUMN balance DECIMAL(18,2) NOT NULL DEFAULT 0",
      "ALTER TABLE `user` ADD INDEX idx_name (name)",
      "ALTER TABLE `user` DROP INDEX idx_name",
      "ALTER TABLE `user` RENAME TO `user_v2`",

      // SELECT with JOIN/GROUP/HAVING/ORDER/LIMIT
      "SELECT DISTINCT u.id, u.name, SUM(o.amount) AS total FROM `user` u LEFT JOIN `order` o ON o.user_id = u.id " +
      "WHERE u.status = 'A' AND o.created_at >= '2024-01-01' GROUP BY u.id, u.name HAVING SUM(o.amount) > 100 " +
      "ORDER BY total DESC, u.id ASC LIMIT 10 OFFSET 20",

      // UNION ALL
      "SELECT id, name FROM a UNION ALL SELECT id, name FROM b",

      // INSERT values / select / set
      "INSERT INTO user(id, name) VALUES (1,'x'),(2,'y')",
      "INSERT INTO user(id, name) SELECT id, name FROM user_v2",
      "INSERT INTO user SET id=3, name='z'",

      // SHOW / DESCRIBE
      "SHOW CREATE TABLE `user`",
      "DESCRIBE `user`",
      "DESC TABLE `user`",
      "SHOW COLUMNS FROM `user`"
    };

    int ok = 0;
    int i = 1;
    for (String sql : sqls) {
      try {
        gp.parse(sql);
        System.out.println("[OK] (" + (i++) + ") " + sql.replace('\n',' ').trim());
        ok++;
      } catch (Exception e) {
        System.out.println("[FAIL] (" + (i++) + ") " + sql);
        e.printStackTrace(System.out);
      }
    }
    if (ok != sqls.length) {
      System.exit(2);
    }
  }

  private static String loadGrammar() throws IOException {
    // Try classpath resource first
    try (InputStream in = SqlParser.class.getResourceAsStream("/jimsql.g4")) {
      if (in != null) {
        byte[] buf = in.readAllBytes();
        return new String(buf, StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      // fallthrough
    }
    // Fallback to source path (useful when running in IDE)
    return Files.readString(Paths.get("server/src/main/resources/jimsql.g4"));
  }
}
