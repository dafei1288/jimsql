package net.fly78.miniSQL.javaccform;

import java.io.StringReader;

public class Test {
	
	public static void main(String[] args) throws ParseException{
		FormsPlSql fpsql = new FormsPlSql(new StringReader("Select * from ttt"));
		
		fpsql.SelectStatement();
	}
	

}
