import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import org.snt.inmemantlr.GenericParser;
import org.snt.inmemantlr.exceptions.*;
import org.snt.inmemantlr.listener.DefaultTreeListener;
import org.snt.inmemantlr.comp.DefaultCompilerOptionsProvider;

public class ParseSmokeTest {
  public static void main(String[] args) throws Exception {
    String grammar = new String(Files.readAllBytes(Paths.get("server/src/main/resources/jimsql.g4")), StandardCharsets.UTF_8);
    GenericParser gp = new GenericParser(grammar);
    DefaultCompilerOptionsProvider cop = new DefaultCompilerOptionsProvider();
    cop.getOptions().clear();
    cop.getOptions().add("-source");
    cop.getOptions().add("8");
    cop.getOptions().add("-target");
    cop.getOptions().add("8");
    gp.setCompilerOptionsProvider(cop);
    gp.compile();

    String[] sqls = new String[] {
      "SELECT DISTINCT `id`, \"name\" FROM `user` WHERE status = 'A' AND age >= 18 ORDER BY id DESC, name ASC LIMIT 10 OFFSET 5;",
      "SELECT * FROM \"user\" WHERE name LIKE 'A%' OR id IN (1,2,3);",
      "SELECT a.id, b.name FROM a INNER JOIN b ON a.id = b.id WHERE a.id BETWEEN 1 AND 10;",
      "INSERT INTO user(id, name) VALUES (1, 'x'), (2, 'y');"
    };

    int ok = 0;
    for (String sql : sqls) {
      DefaultTreeListener l = new DefaultTreeListener();
      gp.setListener(l);
      try {
        gp.parse(sql);
        ok++;
        System.out.println("OK: " + sql);
      } catch (Exception e) {
        System.out.println("FAIL: " + sql);
        e.printStackTrace(System.out);
      }
    }
    if (ok != sqls.length) {
      System.exit(2);
    }
  }
}

