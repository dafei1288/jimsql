package net.fly78.miniSQL.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.fly78.miniSQL.parser.engine.SyntaxItem;

public class StatementProcess {
	private String cmdStr;
	
	
	private void help_input_cmd(){
		this.cmdStr = this.cmdStr == null ? "" :this.cmdStr.toLowerCase();
		//this.Analyze(this.cmdStr);
	}
	
	
/*	
	 public  boolean Analyze(String ACode)
     {
         if (ACode == null)
             return false;
         String vCode = ACode;
         boolean vFind = true;
         while (vFind && (vCode.length() > 0))
         {
             vFind = false;
             for (SyntaxItem vSyntaxItem : SyntaxItem.getSyntaxItems())
             {	
            	 System.out.println();
//System.out.println(vSyntaxItem.getFormPattern());
//System.out.println(vCode);
            	 Pattern pattern = Pattern.compile(vSyntaxItem.getFormPattern());
            	 Matcher matcher = pattern.matcher(vCode);
            	 boolean b= matcher.find();//.matches();
//System.out.println(b);
//System.out.println();
                 if (b)
                 {
//                	 System.out.println();
//                	 System.out.println("切分之前 == 》"+vCode);
                	 
                	 System.out.println(matcher.group());
                	 System.out.println(vSyntaxItem.getName());
                	 
                     vCode = matcher.replaceFirst("");
                     
//                     System.out.println("替换以后 ==》"+vCode);
                     
                     vFind = true;
                     
                    
                     
                     System.out.println();
                     break;
                 }
             }
         }
         return true;
     }
*/
	
	
	
	
	
	
	
	
	
	public String getCmdStr() {
		return cmdStr;
	}

	public void setCmdStr(String cmdStr) {
		this.cmdStr = cmdStr;
		this.help_input_cmd();
	}

	public StatementProcess(String cmd){
		super();
		this.cmdStr = cmd;
		this.help_input_cmd();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
