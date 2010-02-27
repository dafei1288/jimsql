package net.fly78.miniSQL.dbengine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.fly78.dafei1288.util.properties.EnvironmentConfig;
import net.fly78.miniSQL.datatype.DataType;
import net.fly78.miniSQL.dbengine.command.Command;
import net.fly78.miniSQL.dbengine.command.Select;

public class Connection {
	Logger logger =  LoggerFactory.getLogger(Connection.class);
//	private String database_name = "";
//	private String database_path = "";
	private File databasefile ; //数据库目录
	private File storge;        //存储引擎目录
	private File currenttable;  //当前表文件
	private static Connection connection; //连接
	
	/*
	private Command select ; 
	private Command update ;
	private Command insert ;
	private Command delete ;
	private Command alert ;
	private Command show ;
	private Command use ;
	private Command quit ; 
	*/
	/*	
	EnvironmentConfig.getInstance("classmap").getPropertyValue("select");
	public  String UPDATE = EnvironmentConfig.getInstance("classmap").getPropertyValue("update");
	public static final String INSERT = EnvironmentConfig.getInstance("classmap").getPropertyValue("insert");
	public static final String DELETE = EnvironmentConfig.getInstance("classmap").getPropertyValue("delete");
	public static final String ALERT = EnvironmentConfig.getInstance("classmap").getPropertyValue("alert");
	public static final String SHOW = EnvironmentConfig.getInstance("classmap").getPropertyValue("show");
	public static final String USE = EnvironmentConfig.getInstance("classmap").getPropertyValue("use");
	public static final String QUIT = EnvironmentConfig.getInstance("classmap").getPropertyValue("quit");
	*/
	public File getDatabasefile() {
		return databasefile;
	}
	public void setDatabasefile(File databasefile) {
		this.databasefile = databasefile;
	}
	public File getStorge() {
		return storge;
	}
	public void setStorge(File storge) {
		this.storge = storge;
	}
	public File getCurrenttable() {
		return currenttable;
	}
	public void setCurrenttable(File currenttable) {
		this.currenttable = currenttable;
	}
	public static Connection getInstance() throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		if(connection==null){
			connection = new Connection();
		}
		return connection;
	}
	
	
	
	
	
	public File getTable(String name){
		
		currenttable = new File(storge.getName()+File.separatorChar+databasefile.getName()+File.separatorChar+name);
		//System.out.println(currenttable.getPath());
		return currenttable;
	}
	
	private Connection() throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		EnvironmentConfig.getInstance().setFileName("config.properties");
		String r = EnvironmentConfig.getInstance().getPropertyValue("storage");
		storge = new File(r);
		
		if(!storge.exists()){
			storge.mkdir();
		}
		
		databasefile = new File("");
		currenttable = new File("");
		/*
		select = (Command) Class.forName(EnvironmentConfig.getInstance("classmap").getPropertyValue("select")).newInstance();
		update = (Command) Class.forName(EnvironmentConfig.getInstance("classmap").getPropertyValue("update")).newInstance(); ;
		insert = (Command) Class.forName(EnvironmentConfig.getInstance("classmap").getPropertyValue("insert")).newInstance();;
		delete = (Command) Class.forName(EnvironmentConfig.getInstance("classmap").getPropertyValue("delete")).newInstance();;
		alert = (Command) Class.forName(EnvironmentConfig.getInstance("classmap").getPropertyValue("alert")).newInstance();;
		show = (Command) Class.forName(EnvironmentConfig.getInstance("classmap").getPropertyValue("show")).newInstance();;
		use = (Command) Class.forName(EnvironmentConfig.getInstance("classmap").getPropertyValue("use")).newInstance();;
		quit = (Command) Class.forName(EnvironmentConfig.getInstance("classmap").getPropertyValue("quit")).newInstance();; 
		 */
	}
	
	public String toString(){
		return "Connect to storge "+storge.getPath()+" @"+this.hashCode();
	}


	public void excute(String str) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		logger.info(str);
		Command rc = Ref2Class.getCommand(str.toLowerCase().split(" ")[0]);
		str = str.substring(0, str.length()-1);
		rc.excuse(str);
	}
	
	public List<List<DataType>> excuteQuery(String str) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		logger.info(str);
		Select rc = (Select) Ref2Class.getCommand(str.toLowerCase().split(" ")[0]);
		str = str.substring(0, str.length()-1);
		rc.excuse(str);
		return rc.getAl();
		
	}
	
	public void useDB(String ndb){
//		System.out.println("b u = "+databasefile.getPath());
		File tmpt =new File(storge.getName()+File.separatorChar+ndb);
		if(!tmpt.exists()){
			System.out.println("unknow database");
			return;
		}
		
		databasefile = new File(storge.getName()+File.separatorChar+ndb);
		System.out.println("database changed");
//		System.out.println("a f = "+databasefile.getPath());
	}
	
	public void showTables(){
		if(!databasefile.exists())return;
		String[] fl = databasefile.list();
		for(String str : fl){
			System.out.println(str);
		}
	}
	
	public void showDBs(){
		if(!storge.exists())return;
		String[] fl = storge.list();
		for(String str : fl){
			System.out.println(str);
		}
	}
	
	public boolean createDB(String name){
		boolean b = false;
		File tmpt =new File(storge.getName()+File.separatorChar+name);
		if(tmpt.exists()){
			System.out.println("database allready exits");
			return b;
		}
		
		b = tmpt.mkdir();
		System.out.println("database created");
		return b;
	}
	
	public boolean createTable(String name){
		boolean b = false;
		
		return b;
	}
	
	public void describe(String table){
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(this.getTable(table)));
			
			String init_table = br.readLine();
			int cols = Integer.parseInt(init_table.split(" ")[0]);
			for(int i=0;i<cols;i++){
				String tabled = br.readLine();
				
				System.out.println(tabled);
			}
			
			
		
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
			System.out.println("unknow table");
		} catch (IOException e) {
			System.out.println("read table error");
			e.printStackTrace();
		}
		
	}
	
	
	
	public static void main(String[] arg) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		Connection c = new Connection();
		c.showTables();
		c.showDBs();
		c.useDB("db3");
		c.useDB("db4");
		
		c.useDB("db2");
		c.showTables();
		
		c.useDB("db1");
		c.showTables();
		
		System.out.println("----------------------------");
		c.describe("info");
		
		System.out.println();
		System.out.println("----------------------------");
		c.describe("salary");
		
	}
}
