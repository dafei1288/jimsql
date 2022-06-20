import java.io.IOException;
import java.io.InputStream;
import org.snt.inmemantlr.GenericParser;
import org.snt.inmemantlr.exceptions.CompilationException;
import org.snt.inmemantlr.listener.DefaultTreeListener;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;
import org.snt.inmemantlr.tree.ParseTreeProcessor;
import org.snt.inmemantlr.utils.FileUtils;

public class Test {

  public static void main(String[] args) throws Exception {
    String sgrammarcontent = "";
    try (InputStream sgrammar = Test.class.getResourceAsStream("jimsql.g4")) {
      sgrammarcontent = FileUtils.getStringFromStream(sgrammar);
    }

    GenericParser gp = new GenericParser(sgrammarcontent);
    DefaultTreeListener t = new DefaultTreeListener();

    gp.setListener(t);

    boolean compile;
    try {
      gp.compile();
      compile = true;
    } catch (CompilationException e) {
      compile = false;
    }

    String sql = "select id,name,age from t1";
    sql = "show table descript t1";
    ParseTree pt;
    gp.parse(sql);
    pt = t.getParseTree();

    ParseTreeProcessor<String, Object> processor = new ParseTreeProcessor<String, Object>(pt) {



      int cnt = 0;

      @Override
      public String getResult() {

        this.smap.entrySet().forEach(it->{
          System.out.println(it.getKey().getRule()+" ==> "+it.getValue());
        });

        return String.valueOf(cnt);
      }

      @Override
      protected void initialize() {
        for (ParseTreeNode n : this.parseTree.getNodes()) {

          if(n.getRule().equals("columnName")){
            System.out.println("==>"+n.getLabel());
          }
          if(n.getRule().equals("tableName")){
            System.out.println("==>"+n.getLabel());
          }

          smap.put(n,n.getLabel());
        }
      }

      @Override
      protected void process(ParseTreeNode n) {
        cnt++;
        simpleProp(n);

      }
    };

    processor.process();
//    System.out.println(processor.getResult());

  }
}
