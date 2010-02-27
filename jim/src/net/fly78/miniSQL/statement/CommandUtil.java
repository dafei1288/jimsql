package net.fly78.miniSQL.statement;

public enum CommandUtil {
	continued(new Continue()),quit(new Quit());
	
	private Statement stmt;
	CommandUtil(Statement stmt){
		this.stmt = stmt;
	}
	
	public Statement getStatement(){
		return this.stmt;
	}
}
