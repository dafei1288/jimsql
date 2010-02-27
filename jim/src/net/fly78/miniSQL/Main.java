package net.fly78.miniSQL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.fly78.miniSQL.dbengine.Connection;
import net.fly78.miniSQL.parser.Parser;
import net.fly78.miniSQL.parser.ParserException;
import net.fly78.miniSQL.parser.ParserManager;
import net.fly78.miniSQL.statement.Quit;
import net.fly78.miniSQL.statement.Statement;

public class Main {
	public final static Logger logger = LoggerFactory.getLogger(Main.class);

	public final static String NAME = "jimSQL";
	public final static String AUTHOR = "Jack Li";
	public final static String EMAIL = "dafei1288@gmail.com";
	public final static String CLITAG = "jimSQL>";

	public static final void displayInfo() {
		System.out.println("Wellcome to use " + NAME + " command line!");
		System.out.println("author : " + AUTHOR);
		System.out.println("email : " + EMAIL);

	}

	public static final void showCliTag() {
		System.out.print(CLITAG);
	}

	public static void main(String[] args) {
		logger.info(net.fly78.dafei1288.util.date.DateOption
				.data2String(new Date())
				+ " " + NAME + " start");
		Main.displayInfo();
		Main.showCliTag();
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String cmd = "";

		Parser p = new ParserManager();
		Statement stmt = null;

		try {
			while (!((stmt = p.parse(br)) instanceof Quit)) {
				Main.showCliTag();
			}
		} catch (Exception e) {
			System.out.println("database engine init error....");
		}
		System.out.println("Bye .... ");

	}

}
