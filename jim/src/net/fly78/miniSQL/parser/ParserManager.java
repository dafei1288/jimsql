package net.fly78.miniSQL.parser;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.fly78.miniSQL.dbengine.Connection;
import net.fly78.miniSQL.dbengine.Ref2Class;
import net.fly78.miniSQL.dbengine.command.Command;
import net.fly78.miniSQL.statement.CommandUtil;
import net.fly78.miniSQL.statement.Quit;
import net.fly78.miniSQL.statement.Statement;

public class ParserManager implements Parser {
	Logger logger =  LoggerFactory.getLogger(ParserManager.class);

	//√¸¡Óª∫≥Â
	private StringBuffer cmd = new StringBuffer();
	private Connection conn = null;
	
	
	@Override
	public Statement parse(Reader statementReader) throws ParserException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		BufferedReader br = (BufferedReader) statementReader;
		conn = Connection.getInstance();
		//Statment nextCommand = CommandUtil.continued.getStatement()
		
		try {
			
			
			//∂¡»Î√¸¡Ó
			//String res = null;
			String res = br.readLine();
			//ø’◊”◊÷∑˚
			//if (!StringUtils.isNotEmpty(res)) return CommandUtil.continued.getStatement();

			
			//ªÒ»°◊÷∑˚
			if(!StringUtils.endsWith(res, Tokens.END_COMMAND_CHAR )){
				cmd.append(res);
				return CommandUtil.continued.getStatement();
			}
			//◊È∫œ√¸¡Ó
			if(cmd.length()!=0){
				res =cmd.toString()+res;
			}
			//System.out.println("command ==> "+res);
			
			if(StringUtils.endsWithIgnoreCase(res, "quit;")){
				return CommandUtil.quit.getStatement();
			}
			
			try{
				logger.info(res);
				Command rc = Ref2Class.getCommand(res.toLowerCase().split(" ")[0]);
				res = res.substring(0, res.length()-1);
				rc.excuse(res);
			}catch(Exception e){
				System.out.println("unknow command");
			}
			
			/*ÕÀ“€√¸¡Ó «®“∆µΩcommand∞¸÷¥––Ω‚Œˆ√¸¡Ó
			
			StatementProcess sp = new StatementProcess(res);
			
			if(StringUtils.equalsIgnoreCase(res, "show database;")){
				conn.showDBs();
			}
			
			else if(StringUtils.equalsIgnoreCase(res, "show tables;")){
				conn.showTables();
			}
			
			else if(StringUtils.startsWithIgnoreCase(res, "describe")){
				String tablename = res.split(" ")[1].replace(";", "");
				conn.describe(tablename);
			}
			else if(StringUtils.startsWithIgnoreCase(res, "use")){
				String dbname = res.split(" ")[1].replace(";", "");
				conn.useDB(dbname);
			}
			
			else if(StringUtils.startsWithIgnoreCase(res, "create database")){
				String dbname = res.split(" ")[2].replace(";", "");
				conn.createDB(dbname);
				
			}
			//ÕÀ≥ˆ
			else if(StringUtils.endsWithIgnoreCase(res, "quit;")){
				return CommandUtil.quit.getStatement();
			}
			else{
				System.out.println("unknow command");
			}
			*/
			
			//«Â¿Ìª∫≥Â√¸¡Ó
			cmd.setLength(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return CommandUtil.continued.getStatement();
	}


	

}
